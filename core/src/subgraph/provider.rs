use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use graph::data::subgraph::schema::SubgraphEntity;
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};
use graph_graphql::prelude::parser_schema::{Definition, Document, TypeDefinition};
use graph_graphql::schema::ast as sast;

pub struct SubgraphProvider<L, S> {
    logger: Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphId>>>,
    store: Arc<S>,
}

impl<L, S> SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    pub fn new(logger: Logger, resolver: Arc<L>, store: Arc<S>) -> Self {
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        SubgraphProvider {
            logger: logger.new(o!("component" => "SubgraphProvider")),
            event_stream: Some(event_stream),
            event_sink,
            resolver,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            store,
        }
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphProvider {
            logger: self.logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            resolver: self.resolver.clone(),
            subgraphs_running: self.subgraphs_running.clone(),
            store: self.store.clone(),
        }
    }
}

impl<L, S> SubgraphProviderTrait for SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    fn start(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let self_clone = self.clone();

        let link = format!("/ipfs/{}", id);

        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(move |subgraph| -> Box<Future<Item = _, Error = _> + Send> {
                    // If subgraph ID already in set
                    if !self_clone
                        .subgraphs_running
                        .lock()
                        .unwrap()
                        .insert(subgraph.id.clone())
                    {
                        return Box::new(future::err(SubgraphProviderError::AlreadyRunning(
                            subgraph.id,
                        )));
                    }

                    // Place subgraph info into store
                    let created_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let entity_ops =
                        SubgraphEntity::new(&subgraph, SubgraphStatus::Syncing, 0, 0, created_at)
                            .write_operations();
                    self_clone
                        .store
                        .clone()
                        .apply_entity_operations(entity_ops, EventSource::None)
                        .map_err(|err| {
                            error!(
                                self_clone.logger,
                                "Failed to write subgraph to store: {}", err
                            )
                        })
                        .ok();

                    // Build indexes for each entity attribute in the Subgraph
                    let indexing_ops = attribute_indexing_operations(
                        subgraph.id.clone(),
                        subgraph.schema.document.clone(),
                    );
                    self_clone
                        .store
                        .clone()
                        .build_entity_attribute_indexes(indexing_ops)
                        .map_err(|err| {
                            debug!(
                                self_clone.logger,
                                "No subgraph entity indexes created: {}", err
                            )
                        })
                        .map(|_| {
                            info!(
                                self_clone.logger,
                                "Successfully created indexes for all attributes"
                            )
                        })
                        .ok();

                    // Send events to trigger subgraph processing
                    Box::new(
                        self_clone
                            .event_sink
                            .clone()
                            .send(SubgraphProviderEvent::SubgraphStart(subgraph))
                            .map_err(|e| panic!("failed to forward subgraph: {}", e))
                            .map(|_| ()),
                    )
                }),
        )
    }

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            Box::new(
                self.event_sink
                    .clone()
                    .send(SubgraphProviderEvent::SubgraphStop(id))
                    .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                    .map(|_| ()),
            )
        } else {
            Box::new(future::err(SubgraphProviderError::NotRunning(id)))
        }
    }
}

impl<L, S> EventProducer<SubgraphProviderEvent> for SubgraphProvider<L, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>)
    }
}

fn attribute_indexing_operations(
    subgraph_id: SubgraphId,
    document: Document,
) -> Vec<AttributeIndexOperation> {
    let mut indexing_ops = vec![];
    for (entity_number, schema_type) in document.definitions.clone().into_iter().enumerate() {
        if let Definition::TypeDefinition(definition) = schema_type {
            if let TypeDefinition::Object(schema_object) = definition {
                for (attribute_number, entity_field) in schema_object.fields.into_iter().enumerate()
                {
                    let field_value_type =
                        match sast::get_valid_value_type(&entity_field.field_type) {
                            Ok(value_type) => value_type,
                            Err(_) => continue,
                        };
                    let (index_type, index_operator) = match field_value_type {
                        ValueType::Boolean
                        | ValueType::BigInt
                        | ValueType::Bytes
                        | ValueType::Float
                        | ValueType::ID
                        | ValueType::Int => (String::from("btree"), String::from("")),
                        ValueType::String => (String::from("gin"), String::from("gin_trgm_ops")),
                    };

                    indexing_ops.push(AttributeIndexOperation {
                        subgraph_id: subgraph_id.clone(),
                        index_name: format!(
                            "{}_{}_{}_idx",
                            subgraph_id.clone(),
                            entity_number,
                            attribute_number,
                        ),
                        index_type,
                        index_operator,
                        attribute_name: entity_field.name,
                        entity_name: schema_object.name.clone(),
                    });
                }
            }
        }
    }
    indexing_ops
}
