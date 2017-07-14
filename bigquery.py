from google.cloud import bigquery

current = {
  'client': False,
  'dataset': False,
  'table': False
}

def connect(service_account):
  current['client'] = bigquery.Client.from_service_account_json(service_account)

def select_dataset(name):
  for dataset in current['client'].list_datasets():

    if dataset.name == name:
      current['dataset'] = dataset
      return True

  return False

def select_table(name):
  for table in current['dataset'].list_tables():

    if table.name == name:
      current['table'] = table
      return True

  return False

def recreate_table(data):
  current['table'].reload()
  schema = current['table'].schema[:]
  name = current['table'].name
  current['table'].delete()
  new_table = current['dataset'].table(name, schema)
  new_table.create()
  new_table.upload_from_file(data, 'CSV', skip_leading_rows=1, allow_jagged_rows=True, allow_quoted_newlines=True)
  current['table'] = new_table

def list_errors():
  jobs = current['client'].list_jobs()

  for job in jobs:

    if job.error_result is not None:
      yield job.destination.name, job.error_result['message']

    errors = job.errors or ()

    for error in errors:
      yield error['message']