#!/usr/bin/env bash

/archiver --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --bucket ${BUCKET} --labeledtopic ${LABELED_TOPIC} --targetlabel ${TARGET_LABEL}

exit 0
