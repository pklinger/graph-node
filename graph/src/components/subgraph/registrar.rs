use prelude::*;

/// Common trait for named subgraph providers.
pub trait SubgraphRegistrar: Send + Sync + 'static {
    fn create_subgraph(
        &self,
        name: SubgraphDeploymentName,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn create_subgraph_version(
        &self,
        name: SubgraphDeploymentName,
        id: SubgraphId,
        deployment_node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn remove_subgraph(
        &self,
        name: SubgraphDeploymentName,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn list(&self) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error>;
}
