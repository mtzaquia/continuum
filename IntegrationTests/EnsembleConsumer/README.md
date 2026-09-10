# Ensemble consumer check

This opt-in package verifies the public composition API against
[Ensemble](https://github.com/mtzaquia/ensemble), a separate SwiftUI
presentation-state library, without adding it to Continuum's dependencies. It tests
initial loading silence, reset-before-failure clearing, and reload recovery on
the same subscription through three levels of nested compositions. It also
checks direct bucket binding, including initial silence and reset clearing.

By default, Ensemble must be checked out beside Continuum. From Continuum's root:

```sh
swift test --package-path IntegrationTests/EnsembleConsumer
```

For another location, set `ENSEMBLE_PACKAGE_PATH` to the absolute checkout path.
