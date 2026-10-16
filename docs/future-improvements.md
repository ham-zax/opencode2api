# Future improvements

Proposals from the 2026-10-16 repair. These proposals are not implemented.

- Add an opt-in live smoke check with genuine OpenCode requests on Chat and Responses. Synthetic checks silently misreported all models as unavailable.
- Compare source and test-fixture hashes before ARM deployment. Older ARM fixture helpers caused two test failures during this repair.
- Add a CI job for the existing `bun test` command. The current workflow files contain no test invocation.
