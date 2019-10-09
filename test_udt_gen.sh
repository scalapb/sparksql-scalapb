#!/usr/bin/env bash
set -e

VERSION=$(awk -e '{ print $5 }' version.sbt | tr -d '"')

sbt +publishLocal
cp version.sbt test/
cd test/
sbt -Dplugin.version=$VERSION "${@-test}"
