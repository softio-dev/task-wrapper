# Publishing

This project publishes to Maven Central through the `maven-central-release`
profile and the Sonatype Central Publishing Maven Plugin.

## Release Validation

Run the test suite:

```bash
./mvnw test
```

Build the full Central bundle without uploading:

```bash
./mvnw clean verify -Pmaven-central-release -Dcentral.skipPublishing=true
```

This should create the main jar, sources jar, Javadoc jar, POM, and `.asc` signatures under `target/`.

## Release Steps

1. Set a non-SNAPSHOT release version before publishing:

```bash
./mvnw versions:set -DnewVersion=0.1.0
```

2. Run tests:

```bash
./mvnw test
```

3. Upload the deployment to Central for validation:

```bash
./mvnw clean deploy -Pmaven-central-release
```

4. Open the Central Portal deployments page, inspect the validated deployment, and publish it manually.

5. After publishing, move the project back to the next development version:

```bash
./mvnw versions:set -DnewVersion=0.1.1-SNAPSHOT
```

6. Commit the release version change and the next development version change according to the release process you want to use.

## Automatic Publishing

After the first successful manual release, automatic publishing can be enabled by passing:

```bash
./mvnw clean deploy -Pmaven-central-release -Dcentral.autoPublish=true
```

Keep manual publishing for the first release so the Central Portal validation result can be reviewed before immutable publication.
