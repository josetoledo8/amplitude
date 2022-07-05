# README

This is a EL script which copy Amplitude's events into Google BigQuery. However, some manual steps _must be_ executed before run this script.

- **[MANUAL STEP]**: Create a dataset in the BigQuery and name it as `amplitude`
- **[MANUAL STEP]**: Get the API_KEY and SECRET_KEY parameters of your Amplitude's project and pass them to the variables `API_KEY` and `SECRET_KEY` (lines 203, 204). They can be find in Settings > Projects > YOUR_PROJECT in the Amplitude GUI.
- **[MANUAL STEP]**: Pass the name of your GCP project to the variable `BIGQUERY_PROJECT_NAME` (line 199)
- **[MANUAL STEP]**: If you will run this script locally and your PC does not have the GCP auth in the enviroment variables, then in line 3 replace `PATH_TO_YOUR_JSON_KEY` with the correct path to the JSON Key associated to the account service which will manage the copy to the BigQuery. Otherwise, simply comment the line 3.

When the script will be executed, it will make a HTTPS request to the [Amplitude Export API](https://www.docs.developers.amplitude.com/analytics/apis/export-api/#considerations), receive, process and send data to BigQuery. By default, only the yesterday data will be requested, but if you change the variable `CUSTOM_DATE` (line 205) from `None` to a date string `YYYY-MM-DD`, only the events of this day will be copied.

PS. Only a few columns will be copied to BigQuery. Inside the function `generate_events_dict` (line 18) you can find which columns are these.
