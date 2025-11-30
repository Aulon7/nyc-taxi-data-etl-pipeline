Place your GCP service account JSON key here as: gcp-key.json

To create a service account key:
1. Go to GCP Console -> IAM & Admin -> Service Accounts
2. Create a service account with these roles:
    - BigQuery Data Editor
    - BigQuery Job User
   - Storage Object Admin
      or switch to Owner (full accessibility)
3. Create a JSON key and download it
4. Rename it to gcp-key.json and place it in this directory
