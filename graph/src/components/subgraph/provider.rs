use prelude::*;

/// Common trait for subgraph providers.
pub trait SubgraphProvider: EventProducer<SubgraphProviderEvent> + Send + Sync + 'static {
    fn start(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn stop(
        &self,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;
}
