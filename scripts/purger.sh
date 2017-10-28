#!/usr/bin/env bash

/purger --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --purgetopic ${PURGE_TOPIC}

exit 0
