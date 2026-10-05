#!/usr/bin/env bash
# Create or update the Airflow connection secret in AWS Secrets Manager.
# Usage: AWS_REGION=us-east-1 ./scripts/create_secret.sh [secrets/here_graphql.connection.json]
set -euo pipefail

SECRET_NAME="${SECRET_NAME:-airflow/connections/here_graphql}"
FILE="${1:-secrets/here_graphql.connection.json}"
REGION="${AWS_REGION:?set AWS_REGION}"

[[ -f "$FILE" ]] || { echo "Missing $FILE (copy the .example and fill it in)"; exit 1; }
python3 -m json.tool "$FILE" >/dev/null || { echo "$FILE is not valid JSON"; exit 1; }

if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$REGION" >/dev/null 2>&1; then
  aws secretsmanager put-secret-value --secret-id "$SECRET_NAME" --secret-string "file://$FILE" --region "$REGION"
  echo "Updated $SECRET_NAME"
else
  aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string "file://$FILE" --region "$REGION"
  echo "Created $SECRET_NAME"
fi
