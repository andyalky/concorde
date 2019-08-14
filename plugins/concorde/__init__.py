from airflow.plugins_manager import AirflowPlugin

from concorde.operators.googleads_to_s3_operator import GoogleAdsToS3Operator

from concorde.hooks.S3_hook import S3Hook
from concorde.hooks.googleads_hook import GoogleAdsHook

class ConcordePlugin(AirflowPlugin):
    name = "concorde"
    operators = [
        GoogleAdsToS3Operator,
    ]
    hooks = [
        S3Hook,
        GoogleAdsHook,
    ]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []