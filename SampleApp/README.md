# Continuum sample app

Open [SampleApp.xcodeproj](SampleApp.xcodeproj) and run the `SampleApp` scheme on an iOS simulator. The project links the parent directory as a local Swift package.

- Load the catalog to see cached data followed by remote results and persistence. Inject a failure to see retained data and the operation error.
- Edit the catalog with typed mutations, fetch the next page, or reset the snapshot and cache.
- Switch shelves to see separate partition snapshots and loading state.

The app enables `Continuum.debug = .trace` for Console output. The UI tests cover catalog pagination and a partition load.
