#!/usr/bin/env bash

/labeler --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --imagetopic ${IMAGE_TOPIC} --labeledtopic ${LABELED_TOPIC} 

exit 0
