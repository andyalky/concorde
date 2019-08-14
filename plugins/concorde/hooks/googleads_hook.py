from googleads import adwords
from airflow.hooks.base_hook import BaseHook

import yaml

API_VERSION = 'v201809'

class GoogleAdsHook(BaseHook):

    def __init__(self, conn_id):
        connection_object = self.get_connection(conn_id)
        extra_config = connection_object.extra_dejson
        
        auth_string_json = {
            'adwords': {
                'developer_token': extra_config['developer_token'],
                'client_customer_id': extra_config['client_customer_id'],
                'user_agent': extra_config['user_agent'],
                'client_id': extra_config['client_id'],
                'client_secret': extra_config['client_secret'], 
                'refresh_token': extra_config['refresh_token'],
            }
        }

        auth_string_yaml = yaml.dump(auth_string_json)

        self.client = adwords.AdWordsClient.LoadFromString(auth_string_yaml)
        
    def get_report_as_stream(self, report_definition, skip_column_header=True, include_zero_impressions=False):
        report_downloader = self.client.GetReportDownloader(version=API_VERSION)

        return report_downloader.DownloadReportAsStream(
            report_definition, 
            skip_report_header=True, 
            skip_report_summary=True,
            skip_column_header=skip_column_header,            
            include_zero_impressions=include_zero_impressions)

    def get_report_metadata(self, report_name):
        metadata_downloader = self.client.GetService(
            'ReportDefinitionService', version='v201809')

        return metadata_downloader.getReportFields(report_name)            
        
