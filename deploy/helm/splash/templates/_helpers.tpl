{{- define "splash.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "splash.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "splash.classifyUrl" -}}
{{- if .Values.pipeline.classifyUrl -}}
{{- .Values.pipeline.classifyUrl -}}
{{- else -}}
{{- printf "http://%s-classify:%d" (include "splash.fullname" .) (int .Values.classify.service.port) -}}
{{- end -}}
{{- end -}}

{{- define "splash.labels" -}}
app.kubernetes.io/name: {{ include "splash.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
