{{/*
Common labels stamped on every resource. Per-component selector labels are
declared inline in each template (they must stay stable — selectors are
immutable on Deployments/StatefulSets).
*/}}
{{- define "pulse-telemetry.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: pulse-telemetry
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{/*
The database connection URL. database.externalUrl (RDS/Cloud SQL) wins;
otherwise the URL is assembled against the in-chart postgres Service.
Single source of truth for the secret, the migration job, and anything else
that needs the DB.
*/}}
{{- define "pulse-telemetry.databaseUrl" -}}
{{- if .Values.database.externalUrl -}}
{{- .Values.database.externalUrl -}}
{{- else -}}
postgresql://{{ .Values.postgres.auth.username }}:{{ .Values.postgres.auth.password }}@{{ .Release.Name }}-postgres:5432/{{ .Values.postgres.auth.database }}?schema=public
{{- end -}}
{{- end }}

{{/*
Control-plane base URL as seen from inside the cluster (the collector's
pulse_filter sync/stats endpoints hang off this).
*/}}
{{- define "pulse-telemetry.controlPlaneUrl" -}}
http://{{ .Release.Name }}-control-plane:{{ .Values.controlPlane.service.port }}
{{- end }}
