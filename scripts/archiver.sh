#!/usr/bin/env bash

/archiver --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --archivetopic ${ARCHIVE_TOPIC} --purgetopic ${PURGE_TOPIC} --bucket ${BUCKET}

exit 0
