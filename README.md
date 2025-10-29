# AnnoTALE

AnnoTALE is a command-line toolkit for analysing transcription activator-like effector (TALE) proteins.

## Prerequisites

- Java 8 (sources target 1.8)
- Apache Maven 3.6+

- Maven downloads standard plugins from Maven Central, so ensure network access for the first build.

## Repository layout

- `src/main/java` -- Java sources under the `annotale` package
- `src/main/resources` -- runtime assets (TALE HMMs, help text, icons)
- `lib/`, `lib/ssj/`, `lib/xml-commons/` -- bundled third-party jars
- `dist/jstacs-2.5.jar` -- Jstacs dependency required by the CLI
- `scripts/install-local-libs.sh` -- helper that installs all lib jars into a project-local Maven repository (`.m2repo`)

## Building the CLI jar

Because many dependencies are shipped as local jars, install them into the project-local repository before packaging.

```bash
# from the project root
rm -rf .m2repo target          # optional: start from a clean slate
scripts/install-local-libs.sh  # installs dist/ and lib/** jars into .m2repo
mvn package
```

The build produces `target/AnnoTALEcli-1.4.1.jar`. Verify it with:

```bash
java -jar target/AnnoTALEcli-1.4.1.jar --help
```

If the install script is skipped after wiping `.m2repo`, Maven will fail while trying to resolve the `local:*` dependencies from Maven Central. Re-run `scripts/install-local-libs.sh` whenever `.m2repo` is cleaned or jars are added under `lib/` or `dist/`. The repository is configured (via `.mvn/maven.config`) to use `.m2repo` automatically for every Maven invocation.

## Legacy Ant build

The original ant task still lives at `src/AnnoTALEcli.xml`. It rebuilds the same shaded jar into `dist/AnnoTALEcli-1.4.1.jar`.
