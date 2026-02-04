FROM ubuntu:24.04

RUN apt update
RUN apt -y upgrade
RUN apt -y install adduser
RUN apt -y install ca-certificates
RUN update-ca-certificates --fresh
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs/
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN adduser --home $BASEDIR --uid $ID --disabled-password --gecos "Weka User" $USER

WORKDIR $BASEDIR

COPY tarball/export/export $BASEDIR

EXPOSE 8001

WORKDIR $BASEDIR

USER $USER
ENTRYPOINT ["/weka/export"]
