# Load-gen-service

**WARNING**: _This is not an officially support Google product._ 


Service that generates k8s jobs of load-gen-script.


## Setup Instructions:

1. Copy `config.yaml.example` to `config.yaml` and edit with your project info.
    ```sh
    cp config.yaml.example config.yaml
    ```
   
2. Build a docker image containing the `config.yaml` and push to the gcr.io 
repo:
    ```sh 
    docker build --tag=gcr.io/<YOUR-PROJECT>/load-gen-webserver
    docker push gcr.io/<YOUR-PROJECT>/load-gen-webserver
    ```

(_pro tip_: use 
secret to mount the file in the container afterwards instead)

3. Create the _kubernetes service account_ (KSA) with the permissions the 
load-gen-service needs to create jobs:
    ```sh
    kubectl -f apply webserver-service-account.yaml
    ```
   
4. Update ` <YOUR-IMAGE-HERE>` in `webserver-deployment.yaml` with your 
container image name, then apply:
    ```sh
    kubectl -f apply webserver-deployment.yaml
    ```
   
5. _Optional:_ Use a load balancer to expose the deployment externally (Note: 
    consider using _ClusterIP_ type instead for private IP only testing):
   ```
   kubectl -f a
   ```
   
