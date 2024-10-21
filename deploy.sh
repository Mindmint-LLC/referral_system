gcloud run jobs create mm-referral-program-test \
  --image us-central1-docker.pkg.dev/bbg-platform/data-model/mm_referral_program:latest \
  --set-env-vars DBT_DATASET=$DBT_DATASET,DBT_DATASET_STRIPE_MINDMINT=$DBT_DATASET_STRIPE_MINDMINT,DBT_DATASET_STRIPE_MASTERMIND=$DBT_DATASET_STRIPE_MASTERMIND,DBT_PROJECT=$DBT_PROJECT,DBT_KEYFILE=$DBT_KEYFILE,DBT_DATASET_KBBEVERGREEN=$DBT_DATASET_KBBEVERGREEN,MM_API_URL=$MM_API_URL,MM_API_UID=$MM_API_UID,FUNNEL_ID=$FUNNEL_ID,EMAIL_FAIL=$EMAIL_FAIL,EMAIL_UID=$EMAIL_UID,GIT_REPO=$GIT_REPO,GIT_BRANCH=$GIT_BRANCH \
  --set-secrets /volume/bigquery-bbg-platform.json=projects/721464044541/secrets/bigquery-bbg-platform-json/versions/latest \
  --region us-central1