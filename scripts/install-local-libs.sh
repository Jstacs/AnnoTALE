#!/usr/bin/env bash
set -euo pipefail

# Batch-installs bundled third-party jars into the project-local maven repo. The script:
#  - Gather the jars in lib/
#  - Skip anything already present (and newer) in .m2repo
#  - Emit temporary POM with install executions for the rest
#  - Let a single mvn run do the installs

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest_repo="${MAVEN_REPO_DIR:-${project_root}/.m2repo}"

tmpdir="$(mktemp -d 2>/dev/null || mktemp -d -t install-local-libs)"
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

# Core Jstacs dependency alongside other libs
install_jar "lib/jstacs-2.5.jar" "de.jstacs" "jstacs" "2.5"

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
