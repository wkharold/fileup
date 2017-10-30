#!/usr/bin/env bash

/receiver --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --topic ${TOPIC}

exit 0
