# Continuum Sample App

Open `SampleApp.xcodeproj` and run the **SampleApp** scheme. The app links the
parent folder as a local Swift package, so edits to `Sources/Continuum` are
available immediately.

The sample is a guided lab rather than an application shell. Numbered
experiment cards, state badges, and a live plain-language activity feed make it
possible to follow each state transition while interacting with the library:

- **Run the load pipeline** visualizes cache → remote → persistence and includes
  controls for refreshes, failure injection, storage, and persisted resets.
- **Inspect the atomic snapshot** exposes established state, value count,
  mutations, errors, and cursor pagination together.
- **Compare independent partitions** loads each shelf separately to show that
  values and loading state remain scoped to their partition.

`Continuum.debug = .trace` is enabled during app startup to make source,
persistence, and pagination lifecycle events visible in the debug console.
The UI test target covers the catalog pagination and a partition load.
