docker build -t breaking-db-lease .
docker tag breaking-db-lease gcr.io/break-data-bank/breaking-db-lease
docker push gcr.io/break-data-bank/breaking-db-lease
gcloud run deploy breaking-db-lease --platform=managed --port=5000 --allow-unauthenticated --service-account=break-service-test@break-data-bank.iam.gserviceaccount.com --region=us-west1 --update-env-vars=DB_NAME=test,DB_USER=postgres,DB_PASSWORD=postgres,PROD=1 --image=gcr.io/break-data-bank/breaking-db-lease
