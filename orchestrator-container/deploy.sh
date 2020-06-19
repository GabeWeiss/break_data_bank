docker build -t breaking-orchestrator .
docker tag breaking-orchestrator gcr.io/break-data-bank/breaking-orchestrator
docker push gcr.io/break-data-bank/breaking-orchestrator
gcloud run deploy breaking-orchestrator --platform=managed --port=5000 --allow-unauthenticated --service-account=break-service-test@break-data-bank.iam.gserviceaccount.com --region=us-west1 --image=gcr.io/break-data-bank/breaking-orchestrator