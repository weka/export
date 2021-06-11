FROM alpine:latest

RUN apk add --no-cache bash

ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR

WORKDIR $BASEDIR

COPY tarball/export/export $BASEDIR

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR

EXPOSE 8001

USER $USER
ENTRYPOINT ["./export"]
