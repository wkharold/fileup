#!/usr/bin/env bash

/recognizer --filestore ${MINIO_ENDPOINT} --projectid ${PROJECT_ID} --serviceaccount ${SERVICE_ACCOUNT} --imagetopic ${IMAGE_TOPIC} --recognizedtopic ${RECOGNIZED_TOPIC} --purgetopic ${PURGE_TOPIC} --targetlabel ${TARGET_LABEL}

exit 0
