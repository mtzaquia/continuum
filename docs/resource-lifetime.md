# Manage source and observation lifetime

Retain buckets in a repository or feature owner. A `PartitionedBucket` keeps each selected bucket until the container is released. A separately retained bucket can outlive that container. Reset clears a bucket's data while keeping its identity and invalidation subscriptions.

A composition retains its inputs, including the lookup work coordinated by a relationship input. Avoid capturing a feature owner strongly from an input or transform closure if that owner also retains the composition. An external `UpdateSource` owns the tasks or subscriptions it starts.

## Own iterators deliberately

Each bucket or composition iterator retains its source and owns an independent subscription. Cancel the consuming task or release the iterator to end it. Iteration uses unbounded buffering, so a slow consumer can retain every result already emitted. Swift Observation can coalesce changes before emission.

[Partitioning](partitioning.md) · [Compositions](composition.md)
