FROM amazonlinux:latest

RUN yum install -y python3 python3-pip gcc

RUN pip3 install virtualenv

RUN mkdir /build

WORKDIR /build

RUN python3 -m venv env

ENV VIRTUAL_ENV=/build/env
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip3 install google-cloud-bigquery google-auth google-auth-oauthlib
