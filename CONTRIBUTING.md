# Contributing to Oxia

Thank you so much for contributing to Oxia. We appreciate your time and help.
Here are some guidelines to help you get started.

## Code of Conduct

Be kind and respectful to the members of the community. Take time to educate
others who are seeking help. Harassment of any kind will not be tolerated.

## Questions

If you have questions regarding Oxia, feel free to start a new discussion: https://github.com/oxia-db/oxia/discussions/new/choose

## Filing a bug or feature

1. Before filing an issue, please check the existing issues to see if a
   similar one was already opened. If there is one already opened, feel free
   to comment on it.
1. If you believe you've found a bug, please provide detailed steps of
   reproduction, the version of Oxia and anything else you believe will be
   useful to help troubleshoot it (e.g. OS environment, configuration,
   etc...). Also state the current behavior vs. the expected behavior.
1. If you'd like to see a feature or an enhancement please create a new [idea
   discussion](https://github.com/oxia-db/oxia/discussions/new?category=ideas) with
   a clear title and description of what the feature is and why it would be
   beneficial to the project and its users.

## Development setup

Requirements:

* Node.js 18+
* [protoc](https://protobuf.dev/installation/) (on macOS: `brew install protobuf`)
* Docker (for integration tests via testcontainers)

Install dependencies:

```bash
npm ci
```

Generate the protobuf bindings (required once after cloning, and any time
the proto files change):

```bash
npm run gen-proto
```

## Running tests

```bash
# Full suite (integration tests spin up the oxia/oxia:latest container)
npm test

# Unit-only (no Docker required)
npx vitest run test/compare.test.ts

# Watch mode while iterating
npm run test:watch
```

## Linting and formatting

Biome handles both:

```bash
npm run lint        # check only
npm run format      # rewrite in place
```

## Regenerating protobuf bindings

If the Oxia proto definitions change:

```bash
npm run gen-proto
```

## Submitting changes

1. Fork the project.
1. Clone your fork (`git clone https://github.com/your_username/oxia-client-node && cd oxia-client-node`)
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Make changes and run tests (`npm test`)
1. Commit your changes with DCO sign-off (`git commit -s -m 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create a new pull request

**Note:** All commits must include a
[DCO sign-off](https://developercertificate.org/) (`git commit -s`).
