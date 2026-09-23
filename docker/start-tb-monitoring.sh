#!/bin/bash
#
# Copyright © 2016-2026 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CONF_FILE=/usr/share/tb-monitoring/conf/tb-monitoring.conf
LOGBACK_FILE=/usr/share/tb-monitoring/conf/logback.xml

# Absent outside the Helm chart (e.g. a bare `docker run`) - JAVA_OPTS/etc. then just come
# from the container's own env, same as before this script existed.
[ -f "$CONF_FILE" ] && source "$CONF_FILE"

LOGGING_OPT=""
[ -f "$LOGBACK_FILE" ] && LOGGING_OPT="-Dlogging.config=$LOGBACK_FILE"

exec java $JAVA_OPTS $LOGGING_OPT -jar /app/tb-monitoring.jar
