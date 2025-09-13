# deploy the functions

gcloud config set project btibert-ba882-fall25

echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy raw-schema-setup \
    --gen2 \
    --runtime python312 \
    --trigger-http \
    --entry-point task \
    --source ./raw-schema \
    --stage-bucket btibert-ba882-fall25-functions \
    --service-account ba882-fall25@btibert-ba882-fall25.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 