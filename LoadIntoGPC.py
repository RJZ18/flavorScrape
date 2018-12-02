from google.cloud import storage

# Instantiates a client
GOOGLE_APPLICATION_CREDENTIALS = <JSON FILE PATH>
storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

bucket = storage_client.get_bucket('firstproject-218715.appspot.com')

blob = bucket.blob('GPC_API_TEST.txt') #Name of file when it lands on GPC Bucket
blob.upload_from_filename(filename='/home/rjz/Data/YelpAPI') #Local File Name
