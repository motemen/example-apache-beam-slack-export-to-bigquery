# Example: Use Apache Beam to ingest Slack export data into BigQuery

1. Visit https://my.slack.com/services/export to [export workspace data](https://slack.com/help/articles/201658943-Export-your-workspace-data) (You need to be an admin) and save zip file
2. Clone this repository and `poetry install`
3. Set up Google API alongst with Application Default Credentials
   - https://google-auth.readthedocs.io/en/master/reference/google.auth.html
4. Run pipeline...

   - locally:
     ```
     python main.py --project=<project> --output_dataset=<dataset> --temp_location=gs://... --input='./*.zip' --extract_location='./tmp'
     ```
   - using Cloud Dataflow:
     ```
     python main.py --project=<project> --output_dataset=<dataset> --temp_location=gs://... --input='./*.zip' --extract_location='./tmp' --runner=Dataflow --region=<region> --save_main_session
     ```
