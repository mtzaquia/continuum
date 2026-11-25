# Ensemble consumer check

This separate package exercises Continuum with [Ensemble](https://github.com/mtzaquia/ensemble) 3. It covers retained data and errors during refresh, partition retries, resets, and relationship input updates.

From the Continuum checkout, run:

```sh
swift test --package-path IntegrationTests/EnsembleConsumer
```

The fixture uses a sibling `Ensemble` checkout by default and was verified with Ensemble `v3.0.0-beta.3`. Set `ENSEMBLE_PACKAGE_PATH` to use another local checkout.
