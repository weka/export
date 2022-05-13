FROM ubuntu:20.04

RUN apt update
ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN adduser --home $BASEDIR --uid $ID --disabled-password --gecos "Weka User" $USER

WORKDIR $BASEDIR

COPY tarball/export/export $BASEDIR

EXPOSE 8001

USER $USER
ENTRYPOINT ["/weka/export"]
