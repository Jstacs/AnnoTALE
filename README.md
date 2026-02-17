# AnnoTALE

<img src="src/main/resources/annotale/tools/AnnoTALE.png" width="200">

AnnoTALE is a command-line toolkit for analysing transcription activator-like effector (TALE) proteins.

## Prerequisites

- Java 8
- Apache Maven 3.6+

- Maven downloads standard plugins from Maven Central: ensure network access for the first build.

## Repository layout

- `src/main/java` -- Java sources under the `annotale` package
- `src/main/resources` -- runtime assets (TALE HMMs, help text, icons)

## Building the CLI jar

AnnoTALE depends on Jstacs. You need to build and install the dependency into your local maven repository by running `mvn install` in the Jstacs repo root.
Then build the AnnoTALE jar in the AnnoTALE repo root via:

```bash
mvn package
```

The build produces `target/AnnoTALEcli-1.4.1.jar`. Verify it with:

```bash
java -jar target/AnnoTALEcli-1.4.1.jar --help
```

## Legacy Ant build

The original ant task still lives at `src/AnnoTALEcli.xml`. It rebuilds the same shaded jar into `dist/AnnoTALEcli-1.4.1.jar`.

## Licensing information

AnnoTALE is free software: you can redistribute it and/or modify under the terms of the GNU General Public License version 3 or (at your option) any later version as published by the Free Software Foundation.
