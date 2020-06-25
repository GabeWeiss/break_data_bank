docker build -t breaking-loadgen-service .
docker tag breaking-loadgen-service gcr.io/break-data-bank/breaking-loadgen-service
docker push gcr.io/break-data-bank/breaking-loadgen-service
gcloud run deploy breaking-load-service --platform=managed --port=5000 --allow-unauthenticated --service-account=break-service-test@break-data-bank.iam.gserviceaccount.com --region=us-west1 --image=gcr.io/break-data-bank/breaking-loadgen-service