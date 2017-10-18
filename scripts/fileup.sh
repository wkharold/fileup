#!/usr/bin/env bash

/fileup --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --topic ${TOPIC}

exit 0
