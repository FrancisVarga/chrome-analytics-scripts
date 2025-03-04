# Contributing Guidelines

## Conventional Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to automate versioning and changelog generation. Please follow these commit message conventions:

### Format

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

- `feat`: A new feature (minor version bump)
- `fix`: A bug fix (patch version bump)
- `docs`: Documentation changes
- `style`: Changes that don't affect code functionality (formatting, etc.)
- `refactor`: Code changes that neither fix bugs nor add features
- `perf`: Performance improvements
- `test`: Adding or correcting tests
- `chore`: Changes to the build process, tooling, etc.

### Breaking Changes

For breaking changes, add `BREAKING CHANGE:` in the footer followed by a description:

```
feat(api): change authentication method

BREAKING CHANGE: `auth()` now requires an API key parameter
```

Breaking changes will trigger a major version bump.

## Pull Request Process

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes using conventional commit messages
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Release Process

This project uses [release-please](https://github.com/googleapis/release-please) to automate the release process:

1. Commits to the main branch trigger the release-please action
2. Release-please creates or updates a release PR based on conventional commits
3. When the release PR is merged, a new release is created and tagged

To trigger specific version bumps:

- `fix:` commits trigger a patch version bump (0.0.x)
- `feat:` commits trigger a minor version bump (0.x.0)
- Commits with `BREAKING CHANGE:` in the footer trigger a major version bump (x.0.0)

### GitHub Actions Configuration

This project uses several GitHub Actions workflows:

1. **Release Please**: Automates version management and release creation
2. **Auto Merge PR**: Automatically merges PRs with the "automerge" label
3. **Delete Branch After Merge**: Cleans up branches after PRs are merged

#### Personal Access Token Requirement

These workflows require a GitHub Personal Access Token (PAT) with appropriate permissions to create and manage pull requests. The default `GITHUB_TOKEN` provided by GitHub Actions has limitations that prevent it from creating or approving pull requests in some repository configurations.

To set up the required token:

1. Create a GitHub Personal Access Token with `repo` permissions:
   - Go to GitHub Settings > Developer settings > Personal access tokens > Tokens (classic)
   - Generate a new token with `repo` scope
   - Copy the generated token

2. Add the token as a repository secret:
   - Go to your repository Settings > Secrets and variables > Actions
   - Create a new repository secret named `RELEASE_PLEASE_TOKEN`
   - Paste your PAT as the value
