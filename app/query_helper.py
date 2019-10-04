from google.cloud import bigquery

client = bigquery.Client()

example_query = client.query('SELECT * FROM `physionet-data.mimiciii_clinical.prescriptions` WHERE DRUG_TYPE = "MAIN" LIMIT 20')

query_dataframe = example_query.to_dataframe()

print(query_dataframe)