from airflow.models import BaseOperator
from concorde.hooks.S3_hook import S3Hook
from concorde.hooks.googleads_hook import GoogleAdsHook
from airflow.utils.decorators import apply_defaults

import smart_open
import json
import io

class GoogleAdsToS3Operator(BaseOperator):

    #Allow for report definitions and S3 Key Paths to have custom dates
    template_fields = ('source_report_definition', 's3_key')

    @apply_defaults
    def __init__(self,
                 source_conn_id,
                 source_report_definition,                 
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        
        self.source_conn_id = source_conn_id
        self.source_report_definition = source_report_definition     
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        #Create connections to our source and our storage
        self.source_data = GoogleAdsHook(conn_id=self.source_conn_id)
        self.storage = S3Hook(aws_conn_id=self.s3_conn_id) 

        #Get the session and endpoint url
        self.session = self.storage.get_session()
        self.endpoint_url = self.storage.get_endpoint_url()


    def execute(self, context):
        #Query the source for field names and records
        fields = self.get_fields_from_source()
        records = self.get_records_from_source()

        #Write the records to our storage
        self.write_file_to_storage(fields, records)

    #Get columns/field names
    def get_fields_from_source(self):
        return self.source_report_definition['selector']['fields']

    #Get rows/records
    def get_records_from_source(self):
        return self.source_data.get_report_as_stream(self.source_report_definition)

    def write_file_to_storage(self, fields, records):
        #Get parameters to pass to the smart_open open function
        transport_params = {
            'session': self.session,
            'resource_kwargs': {
                'endpoint_url': self.endpoint_url,
            }
        }

        record_stream = io.TextIOWrapper(records, encoding='utf-8', line_buffering=True)

        #Construct the storage URI
        storage_uri = 's3://%s/%s.json.gz' % (self.s3_bucket, self.s3_key)

        #Write records to S3
        with smart_open.open(storage_uri, 'w', transport_params=transport_params) as fout:
            # For each row in the stream
            # Process the row, create a JSON using the zip function and write it to the output file
            for input_row in record_stream.readlines():
                processed_row = input_row.strip().split('\t')
                output_row = dict(zip(fields, processed_row))
                fout.write(json.dumps(output_row) + '\n')