#!/usr/bin/env sh
set -e
meltano install
meltano select tap-mysql ${MYSQL_DATABASE}.* --select '*'
exec meltano run tap-mysql target-bigquery