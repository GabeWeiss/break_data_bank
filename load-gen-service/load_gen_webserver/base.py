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

from kubernetes import client, config
from quart import Blueprint, current_app as app, request


base = Blueprint("base", __name__)

# Setup k8s client
config.load_kube_config()
batch_v1 = client.BatchV1Api()


@base.route("/", methods=["GET"])
async def index():
    return "I am alive!"


def load_gen_container(
    resource_id: str, job_id: str, database_type: str, cloud_sql_ip: str = None,
) -> client.V1Container:
    args = [
        f"--workload-id={job_id}",
        f"--target-type={database_type}",
        f"--database={app.config['DB_NAME']}",
        # TODO: parameterize these from app config
        f"--user={app.config['DB_USER']}",
        f"--password={app.config['DB_PASSWORD']}",
        f"--pubsub_project={app.config['PUBSUB_PROJECT']}",
        f"--pubsub_topic={app.config['PUBSUB_TOPIC']}",
    ]

    if cloud_sql_ip:
        args.append(f"--host={cloud_sql_ip}")

    return client.V1Container(
        name="load-gen",
        args=args,
        # TODO: parameterize this from app config
        image="gcr.io/kvg-testing/load-gen",
    )


@base.route("/", methods=["POST"])
async def create_load_gen_job():

    data = await request.get_json()

    # TODO: add better validation
    job_id = data["job_id"]
    resource_id = data["resource_id"]
    database_type = data["database_type"]
    # read_pattern = data["read_pattern"]
    # write_pattern = data["write_pattern"]
    intensity = data["intensity"]
    cloud_sql_ip = data.get("cloud_sql_ip")

    job = client.V1Job(
        api_version="batch/v1",
        metadata=client.V1ObjectMeta(name=f"test-load-{job_id}"),
        spec=client.V1JobSpec(
            ttl_seconds_after_finished=60,
            # Set # of jobs to run
            completions=intensity,
            parallelism=intensity,
            # Don't retry jobs if they fail
            backoff_limit=0,
            # Container(s) the job should run
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    service_account_name="load-gen-service-account",
                    containers=[
                        load_gen_container(
                            resource_id, job_id, database_type, cloud_sql_ip
                        )
                    ],
                )
            ),
        ),
    )

    batch_v1.create_namespaced_job(body=job, namespace="default")
    return "OK", 200
