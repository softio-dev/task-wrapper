# Repository Guidelines

## Project Structure & Module Organization

This is a Maven-based Spring Boot project. Application code lives under `src/main/java/dev/softio/taskwrapper`, with the entry point in `TaskWrapperApplication.java`. Current modules are grouped by responsibility: `model` for task and queue wrapper types, `producer` for task production DTOs/components, `consumer` for task consumers, and `service` for service-layer logic. Configuration belongs in `src/main/resources/application.properties`. Tests mirror the main package under `src/test/java/dev/softio/taskwrapper`.

## Build, Test, and Development Commands

Use the Maven Wrapper so builds use the repo-supported Maven version:

- `./mvnw test` runs the JUnit and Spring Boot test suite.
- `./mvnw spring-boot:run` starts the application locally.
- `./mvnw package` compiles, tests, and builds the application artifact under `target/`.
- `./mvnw clean` removes generated build output.

The project targets Java 25, as configured in `pom.xml`; use a matching JDK for local development and CI.

## Coding Style & Naming Conventions

Use standard Java formatting with 4-space indentation and package names rooted at `dev.softio.taskwrapper`. Keep class names in `PascalCase`, methods and fields in `camelCase`, and constants in `UPPER_SNAKE_CASE`. Prefer constructor injection for Spring components when dependencies are added. Keep domain wrappers in `model`, application behavior in `service`, and integration-facing producer/consumer code in their existing packages. Lombok is available but optional; use it only when it reduces boilerplate without hiding important behavior.

## Testing Guidelines

Tests use JUnit Jupiter through `spring-boot-starter-test`; Spring context tests use `@SpringBootTest`. Name test classes after the class or behavior under test, for example `TasksServiceTests` or `BasicConsumerTests`. Keep fast unit tests focused on business behavior, and reserve full context or Testcontainers-based tests for Spring wiring and integration scenarios. Run `./mvnw test` before opening a pull request.

## Commit & Pull Request Guidelines

The current Git history starts with a simple `initial commit`, so no strict convention is established yet. Use short, imperative commit messages such as `Add batch task validation` or `Fix consumer startup handling`. Pull requests should include a brief summary, the reason for the change, test results such as `./mvnw test`, and links to related issues when applicable. Include screenshots or logs only for changes that affect runtime behavior or diagnostics.

## Security & Configuration Tips

Do not commit secrets, credentials, or local machine paths. Keep environment-specific values out of `application.properties`; prefer environment variables or externalized Spring configuration for sensitive settings.
