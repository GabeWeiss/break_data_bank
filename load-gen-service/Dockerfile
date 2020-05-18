# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.8-slim

# So I can write to the log with Python easier
ENV PYTHONUNBUFFERED True

# install curl
RUN apt-get update; apt-get install curl --assume-yes

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# Install kubectl so we can access the k8s cluster
RUN gcloud components install kubectl

ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Run the application
ENTRYPOINT ["hypercorn", "load_gen_webserver:create_app()", "--bind", "0.0.0.0:5000" ]
