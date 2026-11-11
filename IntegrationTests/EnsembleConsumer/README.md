# Ensemble consumer check

This opt-in package verifies the public composition API against
[Ensemble](https://github.com/mtzaquia/ensemble), a separate SwiftUI
presentation-state library, without adding it to Continuum's dependencies. The
fixture uses Ensemble 3's `@ViewDataSource` to check that a failed refresh with
retained data delivers success to existing and new bindings while the error stays
observable on the bucket. It also covers sibling changes in a composition,
initial silence, reset-before-failure clearing, and retry for nested
compositions and relationships.

By default, Ensemble must be checked out beside Continuum. The fixture was
verified with Ensemble `v3.0.0-beta.1`. From Continuum's root, run

```sh
swift test --package-path IntegrationTests/EnsembleConsumer
```

For another location, set `ENSEMBLE_PACKAGE_PATH` to the absolute checkout path.
