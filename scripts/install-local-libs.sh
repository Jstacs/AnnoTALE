#!/usr/bin/env bash
set -euo pipefail

# Allow overriding Maven repo location; default to project-local .m2repo
dest_repo="${MAVEN_REPO_DIR:-$(pwd)/.m2repo}"

install_jar() {
  local file="$1" group="$2" artifact="$3" version="$4"
  echo "Installing ${group}:${artifact}:${version}"
  mvn -Dmaven.repo.local="$dest_repo" install:install-file \
    -Dfile="$file" \
    -DgroupId="$group" \
    -DartifactId="$artifact" \
    -Dversion="$version" \
    -Dpackaging=jar \
    -DgeneratePom=true
}

# Core dependency provided in dist/
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

# SSJ stack
install_jar "lib/ssj/colt-1.2.0.jar" "local" "colt" "1.2.0"
install_jar "lib/ssj/commons-math3-3.6.1.jar" "local" "commons-math3" "3.6.1"
install_jar "lib/ssj/concurrent-1.3.4.jar" "local" "concurrent" "1.3.4"
install_jar "lib/ssj/jcommon-1.0.15.jar" "local" "jcommon" "1.0.15"
install_jar "lib/ssj/jfreechart-1.0.12.jar" "local" "jfreechart" "1.0.12"
install_jar "lib/ssj/optimization-1.3.jar" "local" "optimization" "1.3"
install_jar "lib/ssj/ssj-3.3.1.jar" "local" "ssj" "3.3.1"

# Batik & XML Graphics stack
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

# Optional libraries not currently compiled against (skipped: junit and LaTeXlet)
