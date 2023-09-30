# How to publish
1. Bump the version in `pyproject.toml`
2. Tag the commit with the version number e.g. `git tag v0.0.1`
3. Push the tag `git push origin v0.0.1`
4. The GitHub action will publish the package to PyPI
