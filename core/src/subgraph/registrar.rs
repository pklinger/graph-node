use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use graph::data::subgraph::schema::*;
use graph::prelude::{
    SubgraphDeploymentProvider as SubgraphDeploymentProviderTrait,
    SubgraphRegistrar as SubgraphRegistrarTrait, *,
};

pub struct SubgraphRegistrar<L, P, S, CS> {
    logger: Logger,
    resolver: Arc<L>,
    provider: Arc<P>,
    store: Arc<S>,
    chain_store: Arc<CS>,
    node_id: NodeId,
    deployment_event_stream_cancel_guard: CancelGuard, // cancels on drop
}

impl<L, P, S, CS> SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver,
    P: SubgraphDeploymentProviderTrait,
    S: Store,
    CS: ChainStore,
{
    pub fn new(
        logger: Logger,
        resolver: Arc<L>,
        provider: Arc<P>,
        store: Arc<S>,
        chain_store: Arc<CS>,
        node_id: NodeId,
    ) -> Self {
        let logger = logger.new(o!("component" => "SubgraphRegistrar"));

        SubgraphRegistrar {
            logger,
            resolver,
            provider,
            store,
            chain_store,
            node_id,
            deployment_event_stream_cancel_guard: CancelGuard::new(),
        }
    }

    pub fn start(&self) -> impl Future<Item = (), Error = Error> {
        let logger_clone1 = self.logger.clone();
        let logger_clone2 = self.logger.clone();
        let provider = self.provider.clone();
        let node_id = self.node_id.clone();
        let deployment_event_stream_cancel_handle =
            self.deployment_event_stream_cancel_guard.handle();

        // The order of the following three steps is important:
        // - Start deployment event stream
        // - Read deployments table and start deployed subgraphs
        // - Start processing deployment event stream
        //
        // Starting the event stream before reading the deployments table ensures that no
        // deployments are missed in the period of time between the table read and starting event
        // processing.
        // Delaying the start of event processing until after the table has been read and processed
        // ensures that Remove events happen after the deployed subgraphs have been started, not
        // before (otherwise a subgraph could be left running due to a race condition).
        //
        // The discrepancy between the start time of the event stream and the table read can result
        // in some extraneous events on start up. Examples:
        // - The event stream sees an Add event for subgraph A, but the table query finds that
        //   subgraph A is already in the table.
        // - The event stream sees a Remove event for subgraph B, but the table query finds that
        //   subgraph B has already been removed.
        // The `handle_deployment_events` function handles these cases by ignoring AlreadyRunning
        // (on subgraph start) or NotRunning (on subgraph stop) error types, which makes the
        // operations idempotent.

        // Start event stream
        let deployment_event_stream = self.deployment_events();

        // Deploy named subgraphs found in store
        self.start_deployed_subgraphs().and_then(move |()| {
            // Spawn a task to handle deployment events
            tokio::spawn(future::lazy(move || {
                deployment_event_stream
                    .map_err(SubgraphDeploymentProviderError::Unknown)
                    .map_err(CancelableError::Error)
                    .cancelable(&deployment_event_stream_cancel_handle, || {
                        CancelableError::Cancel
                    })
                    .for_each(move |deployment_event| {
                        assert_eq!(deployment_event.node_id(), &node_id);
                        handle_deployment_event(deployment_event, provider.clone(), &logger_clone1)
                    })
                    .map_err(move |e| match e {
                        CancelableError::Cancel => {}
                        CancelableError::Error(e) => {
                            error!(logger_clone2, "deployment event stream failed: {}", e);
                            panic!("deployment event stream error: {}", e);
                        }
                    })
            }));

            Ok(())
        })
    }

    pub fn deployment_events(&self) -> impl Stream<Item = DeploymentEvent, Error = Error> + Send {
        let store = self.store.clone();
        let node_id = self.node_id.clone();

        store
            .subscribe(vec![SubgraphDeploymentEntity::subgraph_entity_pair()])
            .map_err(|()| format_err!("entity change stream failed"))
            .and_then(
                move |entity_change| -> Result<Box<Stream<Item = _, Error = _> + Send>, _> {
                    let subgraph_hash = SubgraphId::new(entity_change.entity_id.clone())
                        .map_err(|()| format_err!("invalid subgraph hash in deployment entity"))?;

                    match entity_change.operation {
                        EntityChangeOperation::Added | EntityChangeOperation::Updated => {
                            store
                                .get(SubgraphDeploymentEntity::key(subgraph_hash.clone()))
                                .map_err(|e| format_err!("failed to get entity: {}", e))
                                .map(|entity_opt| -> Box<Stream<Item = _, Error = _> + Send> {
                                    if let Some(entity) = entity_opt {
                                        if entity.get("nodeId") == Some(&node_id.to_string().into())
                                        {
                                            // Start subgraph on this node
                                            Box::new(stream::once(Ok(DeploymentEvent::Add {
                                                subgraph_id: subgraph_hash,
                                                node_id: node_id.clone(),
                                            })))
                                        } else {
                                            // Ensure it is removed from this node
                                            Box::new(stream::once(Ok(DeploymentEvent::Remove {
                                                subgraph_id: subgraph_hash,
                                                node_id: node_id.clone(),
                                            })))
                                        }
                                    } else {
                                        // Was added/updated, but is now gone.
                                        // We will get a separate Removed event later.
                                        Box::new(stream::empty())
                                    }
                                })
                        }
                        EntityChangeOperation::Removed => {
                            // Send remove event without checking node ID.
                            // If node ID does not match, then this is a no-op when handled in
                            // deployment provider.
                            Ok(Box::new(stream::once(Ok(DeploymentEvent::Remove {
                                subgraph_id: subgraph_hash,
                                node_id: node_id.clone(),
                            }))))
                        }
                    }
                },
            )
            .flatten()
    }

    fn start_deployed_subgraphs(&self) -> impl Future<Item = (), Error = Error> {
        let provider = self.provider.clone();

        // Create a query to find all deployments with this node ID
        let deployment_query = SubgraphDeploymentEntity::query().filter(EntityFilter::Equal(
            "nodeId".to_owned(),
            self.node_id.to_string().into(),
        ));

        future::result(self.store.find(deployment_query))
            .map_err(|e| format_err!("error querying subgraph deployments: {}", e))
            .and_then(move |deployment_entities| {
                deployment_entities
                    .into_iter()
                    .map(|deployment_entity| {
                        // Parse as subgraph hash
                        deployment_entity.id().and_then(|id| {
                            SubgraphId::new(id).map_err(|()| {
                                format_err!("invalid subgraph hash in deployment entity")
                            })
                        })
                    })
                    .collect::<Result<HashSet<SubgraphId>, _>>()
            })
            .and_then(move |subgraph_ids| {
                let provider = provider.clone();
                stream::iter_ok(subgraph_ids).for_each(move |id| provider.start(id).from_err())
            })
    }
}

impl<L, P, S, CS> SubgraphRegistrarTrait for SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver,
    P: SubgraphDeploymentProviderTrait,
    S: Store,
    CS: ChainStore,
{
    fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static> {
        debug!(self.logger, "Creating subgraph: {:?}", name.to_string());

        let mut ops = vec![];

        // Abort if this subgraph already exists
        ops.push(EntityOperation::AbortUnless {
            query: EntityQuery {
                subgraph_id: SUBGRAPHS_ID.clone(),
                entity_type: SubgraphEntity::TYPENAME.to_owned(),
                filter: Some(EntityFilter::Equal(
                    "name".to_owned(),
                    name.to_string().into(),
                )),
                order_by: None,
                order_direction: None,
                range: None,
            },
            entity_ids: vec![],
        });

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let entity = SubgraphEntity::new(name, None, created_at);
        let entity_id = generate_entity_id();
        ops.append(&mut entity.write_operations(&entity_id));

        Box::new(
            future::result(self.store.apply_entity_operations(ops, EventSource::None)).from_err(),
        )
    }

    fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: SubgraphId,
        node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static> {
        let store = self.store.clone();
        let chain_store = self.chain_store.clone();

        debug!(
            self.logger,
            "Writing new subgraph version to store: subgraph name = {:?}, subgraph hash = {:?}",
            name.to_string(),
            hash.to_string()
        );

        let link = format!("/ipfs/{}", hash);

        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphRegistrarError::ResolveError)
                .and_then(move |manifest| {
                    // Look up subgraph by name
                    let subgraph_entities = store
                        .find(SubgraphEntity::query().filter(EntityFilter::Equal(
                            "name".to_owned(),
                            name.to_string().into(),
                        )))
                        .map_err(SubgraphRegistrarError::from)?;
                    let subgraph_entity = match subgraph_entities.len() {
                        0 => Err(SubgraphRegistrarError::NameNotFound(name.to_string())),
                        1 => {
                            let mut subgraph_entities = subgraph_entities;
                            Ok(subgraph_entities.pop().unwrap())
                        }
                        _ => panic!(
                            "multiple subgraph entities with name {:?}",
                            name.to_string()
                        ),
                    }?;
                    let subgraph_entity_id =
                        subgraph_entity.id().map_err(SubgraphRegistrarError::from)?;

                    // Check if subgraph state already exists for this hash
                    let state_entities = store
                        .find(SubgraphStateEntity::query().filter(EntityFilter::Equal(
                            "id".to_owned(),
                            hash.to_string().into(),
                        )))
                        .map_err(SubgraphRegistrarError::from)?;
                    let state_exists = match state_entities.len() {
                        0 => false,
                        1 => true,
                        _ => panic!(
                            "multiple subgraph states found for hash {}",
                            hash.to_string()
                        ),
                    };

                    // Prepare entity ops transaction
                    let mut ops = vec![
                        // Subgraph entity must still exist, have same name
                        EntityOperation::AbortUnless {
                            query: SubgraphEntity::query().filter(EntityFilter::Equal(
                                "name".to_owned(),
                                name.to_string().into(),
                            )),
                            entity_ids: vec![subgraph_entity_id.clone()],
                        },
                        // Subgraph state entity must continue to exist/not exist
                        EntityOperation::AbortUnless {
                            query: SubgraphStateEntity::query().filter(EntityFilter::Equal(
                                "id".to_owned(),
                                hash.to_string().into(),
                            )),
                            entity_ids: if state_exists {
                                vec![hash.to_string()]
                            } else {
                                vec![]
                            },
                        },
                    ];

                    let version_entity_id = generate_entity_id();
                    let created_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    ops.append(
                        &mut SubgraphVersionEntity::new(
                            subgraph_entity_id.clone(),
                            hash.clone(),
                            created_at,
                        )
                        .write_operations(&version_entity_id),
                    );

                    // Create state only if it does not exist already
                    if !state_exists {
                        let chain_head_ptr_opt = chain_store
                            .chain_head_ptr()
                            .map_err(SubgraphRegistrarError::from)?;
                        let chain_head_block_number = match chain_head_ptr_opt {
                            Some(chain_head_ptr) => chain_head_ptr.number,
                            None => 0,
                        };
                        let genesis_block_ptr = chain_store
                            .genesis_block_ptr()
                            .map_err(SubgraphRegistrarError::from)?;
                        ops.append(
                            &mut SubgraphStateEntity::new(
                                &manifest,
                                false,
                                genesis_block_ptr,
                                chain_head_block_number,
                            )
                            .create_operations(&hash),
                        );
                    }

                    ops.append(
                        &mut SubgraphDeploymentEntity::new(node_id, false).write_operations(&hash),
                    );

                    // TODO support delayed update of currentVersion
                    ops.append(&mut SubgraphEntity::update_current_version_operations(
                        &subgraph_entity_id,
                        &version_entity_id,
                    ));

                    // Commit entity ops
                    // TODO retry on abort
                    store
                        .apply_entity_operations(ops, EventSource::None)
                        .map_err(SubgraphRegistrarError::from)
                }),
        )
    }

    fn remove_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static> {
        debug!(self.logger, "Removing subgraph: {:?}", name.to_string());

        // TODO remove other entities related to subgraph
        Box::new(future::result(
            self.store
                .find(EntityQuery {
                    subgraph_id: SUBGRAPHS_ID.clone(),
                    entity_type: SubgraphEntity::TYPENAME.to_owned(),
                    filter: Some(EntityFilter::Equal(
                        "name".to_owned(),
                        name.to_string().into(),
                    )),
                    order_by: None,
                    order_direction: None,
                    range: None,
                })
                .map_err(|e| format_err!("query execution error: {}", e))
                .map_err(SubgraphRegistrarError::from)
                .and_then(|matching_subgraphs| {
                    let subgraph = match matching_subgraphs.as_slice() {
                        [] => Err(SubgraphRegistrarError::NameNotFound(name.to_string())),
                        [subgraph] => Ok(subgraph),
                        _ => panic!("multiple subgraphs with same name in store"),
                    }?;

                    let ops = SubgraphEntity::remove_operations(&subgraph.id()?);
                    self.store
                        .apply_entity_operations(ops, EventSource::None)
                        .map_err(SubgraphRegistrarError::from)
                }),
        ))
    }

    fn list_subgraphs(
        &self,
    ) -> Box<Future<Item = Vec<SubgraphName>, Error = SubgraphRegistrarError> + Send + 'static>
    {
        unimplemented!()
        //self.store.read_by_node_id(self.node_id.clone())
    }
}

fn handle_deployment_event<P>(
    event: DeploymentEvent,
    provider: Arc<P>,
    logger: &Logger,
) -> Box<Future<Item = (), Error = CancelableError<SubgraphDeploymentProviderError>> + Send>
where
    P: SubgraphDeploymentProviderTrait,
{
    let logger = logger.to_owned();

    debug!(logger, "Received deployment event: {:?}", event);

    match event {
        DeploymentEvent::Add {
            subgraph_id,
            node_id: _,
        } => {
            Box::new(
                provider
                    .start(subgraph_id.clone())
                    .then(move |result| -> Result<(), _> {
                        match result {
                            Ok(()) => Ok(()),
                            Err(SubgraphDeploymentProviderError::AlreadyRunning(_)) => Ok(()),
                            Err(e) => {
                                // Errors here are likely an issue with the subgraph.
                                // These will be recorded eventually so that they can be displayed
                                // in a UI.
                                error!(
                                    logger,
                                    "Subgraph instance failed to start: {}", e;
                                    "subgraph_id" => subgraph_id.to_string()
                                );
                                Ok(())
                            }
                        }
                    }),
            )
        }
        DeploymentEvent::Remove {
            subgraph_id,
            node_id: _,
        } => Box::new(
            provider
                .stop(subgraph_id)
                .then(|result| match result {
                    Ok(()) => Ok(()),
                    Err(SubgraphDeploymentProviderError::NotRunning(_)) => Ok(()),
                    Err(e) => Err(e),
                })
                .map_err(CancelableError::Error),
        ),
    }
}
