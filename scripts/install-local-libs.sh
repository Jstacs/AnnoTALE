#!/usr/bin/env bash
set -euo pipefail

# Batch-installs bundled third-party jars into the project-local maven repo. The script:
#  - Gather the jars in dist/ and lib/
#  - Skip anything already present (and newer) in .m2repo
#  - Emit temporary POM with install executions for the rest
#  - Let a single mvn run do the installs

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest_repo="${MAVEN_REPO_DIR:-${project_root}/.m2repo}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

pom="${tmpdir}/install-local-libs.pom"

cat >"$pom" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>de.annotale</groupId>
  <artifactId>install-local-libs</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-install-plugin</artifactId>
        <version>3.1.1</version>
        <executions>
EOF

need_install=0

append_install_execution() { # add execution block for one jar
  local abs_file="$1" group="$2" artifact="$3" version="$4"
  local exec_id="install-${group//./-}-${artifact}-${version}"

  cat >>"$pom" <<EOF
          <execution>
            <id>${exec_id}</id>
            <phase>initialize</phase>
            <goals>
              <goal>install-file</goal>
            </goals>
            <configuration>
              <file>${abs_file}</file>
              <groupId>${group}</groupId>
              <artifactId>${artifact}</artifactId>
              <version>${version}</version>
              <packaging>jar</packaging>
              <generatePom>true</generatePom>
            </configuration>
          </execution>
EOF
}

install_jar() { # schedule jar if missing/old
  local rel_file="$1" group="$2" artifact="$3" version="$4"
  local abs_file="${project_root}/${rel_file}"

  if [[ ! -f "$abs_file" ]]; then
    echo "Missing jar: ${rel_file}" >&2
    exit 1
  fi

  local group_path="${group//./\/}"
  local target="${dest_repo}/${group_path}/${artifact}/${version}/${artifact}-${version}.jar"

  if [[ -f "$target" && "$target" -nt "$abs_file" ]]; then
    echo "Skipping ${group}:${artifact}:${version} (already installed)"
    return
  fi

  echo "Scheduling ${group}:${artifact}:${version}"
  need_install=1
  append_install_execution "$abs_file" "$group" "$artifact" "$version"
}

# Core Jstacs dependency in dist/
install_jar "dist/jstacs-2.5.jar" "de.jstacs" "jstacs" "2.5"

# Root lib directory jars
install_jar "lib/BigWig.jar" "local" "BigWig" "1.0"
install_jar "lib/biojava-live.jar" "local" "biojava-live" "1.0"
install_jar "lib/bytecode-1.9.0.jar" "local" "bytecode" "1.9.0"
install_jar "lib/bytecode.jar" "local" "bytecode-tools" "1.0"
install_jar "lib/core-1.9.0.jar" "local" "core" "1.9.0"
install_jar "lib/htsjdk-2.5.0-4-gd683012-SNAPSHOT.jar" "local" "htsjdk" "2.5.0-4-gd683012-SNAPSHOT"
install_jar "lib/Jama-1.0.3.jar" "local" "Jama" "1.0.3"
install_jar "lib/log4j-1.2.15.jar" "local" "log4j" "1.2.15"
install_jar "lib/numericalMethods.jar" "local" "numericalMethods" "1.0"
install_jar "lib/RClient-0.6.7.jar" "local" "RClient" "0.6.7"
install_jar "lib/LaTeXlet-1.2f8.jar" "local" "LaTeXlet" "1.2f8"

# SSJ
install_jar "lib/ssj/colt-1.2.0.jar" "local" "colt" "1.2.0"
install_jar "lib/ssj/commons-math3-3.6.1.jar" "local" "commons-math3" "3.6.1"
install_jar "lib/ssj/concurrent-1.3.4.jar" "local" "concurrent" "1.3.4"
install_jar "lib/ssj/jcommon-1.0.15.jar" "local" "jcommon" "1.0.15"
install_jar "lib/ssj/jfreechart-1.0.12.jar" "local" "jfreechart" "1.0.12"
install_jar "lib/ssj/optimization-1.3.jar" "local" "optimization" "1.3"
install_jar "lib/ssj/ssj-3.3.1.jar" "local" "ssj" "3.3.1"

# Batik & XML
install_jar "lib/xml-commons/batik-anim.jar" "local" "batik-anim" "1.0"
install_jar "lib/xml-commons/batik-awt-util.jar" "local" "batik-awt-util" "1.0"
install_jar "lib/xml-commons/batik-bridge.jar" "local" "batik-bridge" "1.0"
install_jar "lib/xml-commons/batik-codec.jar" "local" "batik-codec" "1.0"
install_jar "lib/xml-commons/batik-css.jar" "local" "batik-css" "1.0"
install_jar "lib/xml-commons/batik-dom.jar" "local" "batik-dom" "1.0"
install_jar "lib/xml-commons/batik-ext.jar" "local" "batik-ext" "1.0"
install_jar "lib/xml-commons/batik-extension.jar" "local" "batik-extension" "1.0"
install_jar "lib/xml-commons/batik-gui-util.jar" "local" "batik-gui-util" "1.0"
install_jar "lib/xml-commons/batik-gvt.jar" "local" "batik-gvt" "1.0"
install_jar "lib/xml-commons/batik-parser.jar" "local" "batik-parser" "1.0"
install_jar "lib/xml-commons/batik-script.jar" "local" "batik-script" "1.0"
install_jar "lib/xml-commons/batik-svg-dom.jar" "local" "batik-svg-dom" "1.0"
install_jar "lib/xml-commons/batik-svggen.jar" "local" "batik-svggen" "1.0"
install_jar "lib/xml-commons/batik-swing.jar" "local" "batik-swing" "1.0"
install_jar "lib/xml-commons/batik-transcoder.jar" "local" "batik-transcoder" "1.0"
install_jar "lib/xml-commons/batik-util.jar" "local" "batik-util" "1.0"
install_jar "lib/xml-commons/batik-xml.jar" "local" "batik-xml" "1.0"
install_jar "lib/xml-commons/batik.jar" "local" "batik" "1.0"
install_jar "lib/xml-commons/commons-io-1.3.1.jar" "local" "commons-io" "1.3.1"
install_jar "lib/xml-commons/commons-logging-1.0.4.jar" "local" "commons-logging" "1.0.4"
install_jar "lib/xml-commons/pdf-transcoder.jar" "local" "pdf-transcoder" "1.0"
install_jar "lib/xml-commons/xml-apis-ext.jar" "local" "xml-apis-ext" "1.0"
install_jar "lib/xml-commons/xmlgraphics-commons-1.5.jar" "local" "xmlgraphics-commons" "1.5"

cat >>"$pom" <<'EOF'
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
EOF

if (( need_install == 0 )); then
  echo "All local jars already present in ${dest_repo}"
  exit 0
fi

mvn --batch-mode --no-transfer-progress -Dmaven.repo.local="${dest_repo}" -f "${pom}" install

echo "Installed local jars into ${dest_repo}"
