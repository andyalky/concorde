from airflow import DAG

from concorde.operators.googleads_to_s3_operator import GoogleAdsToS3Operator

from datetime import datetime, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('googleads-etl', catchup=False, default_args=default_args)

SOURCE_CONN_ID = 'campaigner_googleads'
S3_CONN_ID = 'campaigner_s3'
S3_BUCKET = 'cmpetl'

reports_to_add = [    
    {   #Ad
        'reportName': 'Ad',
        'reportType': 'AD_PERFORMANCE_REPORT',
        'fields': [
            'AccentColor', 'AdGroupId', 'AdStrengthInfo', 'AdType', 'AllowFlexibleColor', 'Automated', 'BusinessName', 'CallOnlyPhoneNumber', 
            'CallToActionText', 'CampaignId', 'CombinedApprovalStatus', 'CreativeDestinationUrl', 'CreativeFinalAppUrls', 'CreativeFinalMobileUrls', 
            'CreativeFinalUrls', 'CreativeTrackingUrlTemplate', 'CreativeUrlCustomParameters', 'Description', 'Description1', 'Description2', 'DevicePreference',
            'DisplayUrl', 'EnhancedDisplayCreativeLandscapeLogoImageMediaId', 'EnhancedDisplayCreativeLogoImageMediaId', 
            'EnhancedDisplayCreativeMarketingImageMediaId', 'EnhancedDisplayCreativeMarketingImageSquareMediaId', 'ExpandedDynamicSearchCreativeDescription2', 
            'ExpandedTextAdDescription2', 'ExpandedTextAdHeadlinePart3', 'ExternalCustomerId', 'FormatSetting', 'GmailCreativeHeaderImageMediaId', 
            'GmailCreativeLogoImageMediaId', 'GmailCreativeMarketingImageMediaId', 'GmailTeaserBusinessName', 'GmailTeaserDescription', 
            'GmailTeaserHeadline', 'Headline', 'HeadlinePart1', 'HeadlinePart2', 
            #'CreativeId (maps to the Id field in the Google Ads report)', 
            'ImageAdUrl', 'ImageCreativeImageHeight', 'ImageCreativeImageWidth', 'ImageCreativeMimeType', 'ImageCreativeName', 'LabelIds', 
            'Labels', 'LongHeadline', 'MainColor', 'MarketingImageCallToActionText', 'MarketingImageCallToActionTextColor', 'MarketingImageDescription', 
            'MarketingImageHeadline', 'MultiAssetResponsiveDisplayAdAccentColor', 'MultiAssetResponsiveDisplayAdBusinessName', 
            'MultiAssetResponsiveDisplayAdCallToActionText', 'MultiAssetResponsiveDisplayAdDescriptions', 
            'MultiAssetResponsiveDisplayAdDynamicSettingsPricePrefix', 'MultiAssetResponsiveDisplayAdDynamicSettingsPromoText', 
            'MultiAssetResponsiveDisplayAdFormatSetting', 'MultiAssetResponsiveDisplayAdHeadlines', 'MultiAssetResponsiveDisplayAdLandscapeLogoImages', 
            'MultiAssetResponsiveDisplayAdLogoImages', 'MultiAssetResponsiveDisplayAdLongHeadline', 'MultiAssetResponsiveDisplayAdMainColor', 
            'MultiAssetResponsiveDisplayAdMarketingImages', 'MultiAssetResponsiveDisplayAdSquareMarketingImages', 'MultiAssetResponsiveDisplayAdYouTubeVideos', 
            'ResponsiveSearchAdDescriptions', 'ResponsiveSearchAdHeadlines', 'ResponsiveSearchAdPath1', 'ResponsiveSearchAdPath2', 'Path1', 'Path2', 
            'PolicySummary', 'PricePrefix', 'PromoText', 'ShortHeadline', 'Status', 'SystemManagedEntitySource', 'UniversalAppAdDescriptions', 
            'UniversalAppAdHeadlines', 'UniversalAppAdHtml5MediaBundles', 'UniversalAppAdImages', 'UniversalAppAdMandatoryAdText', 'UniversalAppAdYouTubeVideos',
        ],
    },
    {   #Keyword
        'reportName': 'Keyword',
        'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
        'fields': [
            'AdGroupId', 'ApprovalStatus', 'BiddingStrategyId', 'BiddingStrategyName', 'BiddingStrategySource', 'BiddingStrategyType', 
            'CampaignId', 'CpcBid', 'CpcBidSource', 
            'CpmBid',
            'CreativeQualityScore', 'Criteria', 'CriteriaDestinationUrl', 'EnhancedCpcEnabled', 'EstimatedAddClicksAtFirstPositionCpc', 
            'EstimatedAddCostAtFirstPositionCpc', 'ExternalCustomerId', 'FinalAppUrls', 'FinalMobileUrls', 'FinalUrls', 'FirstPageCpc', 'FirstPositionCpc', 
            'HasQualityScore', 
            'Id',
            'IsNegative', 'KeywordMatchType', 'LabelIds', 'Labels', 'PostClickQualityScore', 'QualityScore', 'SearchPredictedCtr', 'Status', 'SystemServingStatus', 
            'TopOfPageCpc', 'TrackingUrlTemplate', 'UrlCustomParameters', 'VerticalId',
        ]
    },    
    
]


#Source is https://cloud.google.com/bigquery/docs/adwords-transformation
reports = [
    {   #AdStats
        'reportName': 'AdStats',
        'reportType': 'AD_PERFORMANCE_REPORT',
        'fields': [
            'ActiveViewCpm', 'ActiveViewCtr', 'ActiveViewImpressions', 'ActiveViewMeasurability', 'ActiveViewMeasurableCost', 'ActiveViewMeasurableImpressions',
            'ActiveViewViewability', 'AdGroupId', 'AdNetworkType1', 'AdNetworkType2', 'AverageCost', 'AverageCpc', 'AverageCpm', 'AveragePosition', 
            'BaseAdGroupId', 'BaseCampaignId', 'CampaignId', 'Clicks', 'ClickType', 'ConversionRate', 'Conversions', 'ConversionValue', 'Cost', 
            'CostPerConversion', 'CostPerCurrentModelAttributedConversion', 'CriterionId', 'CriterionType', 'Ctr', 'CurrentModelAttributedConversions', 
            'CurrentModelAttributedConversionValue', 'Date', 'DayOfWeek', 'Device', 'ExternalCustomerId', 'GmailForwards', 'GmailSaves', 'GmailSecondaryClicks', 
            'Id', 'Impressions', 'InteractionRate', 'Interactions', 'InteractionTypes', 'IsNegative', 
            'Month', 'MonthOfYear', 'Quarter', 'Slot', 'ValuePerConversion', 'ValuePerCurrentModelAttributedConversion', 'Week', 'Year',
        ],
    },
    {   #ClickStats
        'reportName': 'ClickStats',
        'reportType': 'CLICK_PERFORMANCE_REPORT',
        'fields': [
            'AccountDescriptiveName', 'AdFormat', 'AdGroupId', 'AdNetworkType1', 'AdNetworkType2', 'AoiCityCriteriaId', 'AoiCountryCriteriaId',
            'AoiMetroCriteriaId', 'AoiMostSpecificTargetId', 'AoiRegionCriteriaId', 'CampaignId', 'CampaignLocationTargetId', 'Clicks', 
            'ClickType', 'CreativeId', 'CriteriaId', 'CriteriaParameters', 'Date', 'Device', 'ExternalCustomerId', 'GclId', 'KeywordMatchType',
            'LopCityCriteriaId', 'LopCountryCriteriaId', 'LopMetroCriteriaId', 'LopMostSpecificTargetId', 'LopRegionCriteriaId', 
            'MonthOfYear', 'Page', 'Slot', 'UserListId',
        ],
    },
    {   #KeywordStats
        'reportName': 'KeywordStats',
        'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
        'fields': [
            'ActiveViewCpm', 'ActiveViewCtr', 'ActiveViewImpressions', 'ActiveViewMeasurability', 'ActiveViewMeasurableCost', 
            'ActiveViewMeasurableImpressions', 'ActiveViewViewability', 'AdGroupId', 'AdNetworkType1', 'AdNetworkType2', 'AverageCost', 'AverageCpc', 
            'AverageCpm', 'AveragePosition', 'BaseAdGroupId', 'BaseCampaignId', 'CampaignId', 'Clicks', 'ClickType', 'ConversionRate', 'Conversions', 
            'ConversionValue', 'Cost', 'CostPerConversion', 'CostPerCurrentModelAttributedConversion', 'Ctr', 'CurrentModelAttributedConversions', 
            'CurrentModelAttributedConversionValue', 'Date', 'DayOfWeek', 'Device', 'ExternalCustomerId', 'GmailForwards', 'GmailSaves', 'GmailSecondaryClicks', 
            #'CriterionId (maps to the Id field in the Google Ads report)', 
            'Impressions', 'InteractionRate', 'Interactions', 'InteractionTypes', 'Month', 
            'MonthOfYear', 'Quarter', 'Slot', 'ValuePerConversion', 'ValuePerCurrentModelAttributedConversion', 'Week', 'Year',
        ]
    },

    {   #PlacementStats
        'reportName': 'PlacementStats',
        'reportType': 'PLACEMENT_PERFORMANCE_REPORT',
        'fields': [
            'ActiveViewCpm', 'ActiveViewCtr', 'ActiveViewImpressions', 'ActiveViewMeasurability', 'ActiveViewMeasurableCost', 
            'ActiveViewMeasurableImpressions', 'ActiveViewViewability', 'AdGroupId', 'AdNetworkType1', 'AdNetworkType2', 'AllConversionRate', 
            'AllConversions', 'AllConversionValue', 'AverageCost', 'AverageCpc', 'AverageCpm', 'BaseAdGroupId', 'BaseCampaignId', 'CampaignId', 
            'Clicks', 'ClickType', 'ConversionRate', 'Conversions', 'ConversionValue', 'Cost', 'CostPerAllConversion', 'CostPerConversion', 
            #'CriterionId (maps to the Id field in the Google Ads report)', 
            'CrossDeviceConversions', 'Ctr', 'Date', 'DayOfWeek', 'Device', 'ExternalCustomerId', 'GmailForwards', 'GmailSaves', 
            'GmailSecondaryClicks', 'Impressions', 'InteractionRate', 'Interactions', 'InteractionTypes', 'Month', 'MonthOfYear', 
            'Quarter', 'ValuePerAllConversion', 'ValuePerConversion', 'Week', 'Year',
        ],
    },
    
    {   #SearchQueryPerformance
        'reportName': 'SearchQueryStats',
        'reportType': 'SEARCH_QUERY_PERFORMANCE_REPORT',
        'fields': [
            'AbsoluteTopImpressionPercentage', 'AdFormat', 'AdGroupId', 'AdNetworkType1', 'AdNetworkType2', 'AllConversionRate',
            'AllConversions', 'AllConversionValue', 'AverageCost', 'AverageCpc', 'AverageCpe', 'AverageCpm', 'AverageCpv',
            'AveragePosition', 'CampaignId', 'Clicks', 'ConversionRate', 'Conversions', 'ConversionValue', 'Cost', 'CostPerAllConversion',
            'CostPerConversion', 'CreativeId', 'CrossDeviceConversions', 'Ctr', 'Date', 'DayOfWeek', 'Device', 'EngagementRate',
            'Engagements', 'ExternalCustomerId', 'Impressions', 'InteractionRate', 'Interactions', 'InteractionTypes',  
            #'KeywordId', 
            'Month',
            'MonthOfYear', 'Quarter', 'Query', 'QueryMatchTypeWithVariant', 'QueryTargetingStatus', 'TopImpressionPercentage', 'ValuePerAllConversion',
            'ValuePerConversion', 'VideoQuartile100Rate', 'VideoQuartile25Rate', 'VideoQuartile50Rate', 'VideoQuartile75Rate', 'VideoViewRate',
            'VideoViews', 'ViewThroughConversions', 'Week', 'Year'
        ]
    }
]

def prepare_report_definition(report):
    return {
        'reportName': report['reportName'],
        'dateRangeType': 'CUSTOM_DATE',
        'reportType': report['reportType'],
        'downloadFormat': 'TSV',
        'selector': {
            'fields': report['fields'],
            'dateRange': {
                'min': '{{yesterday_ds_nodash}}',
                'max': '{{yesterday_ds_nodash}}',
            }  
        },
    }


for unprocessed_report in reports:

    report = prepare_report_definition(unprocessed_report)

    key_path = '{{macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y/%m/%d")}}'
    key_name = '{0}-{1}-{2}'.format(report['reportName'], '{{yesterday_ds_nodash}}', str(uuid.uuid4()))

    t1 = GoogleAdsToS3Operator(
            task_id = 'extract-{0}'.format(report['reportName']),
            dag = dag,
            source_conn_id = SOURCE_CONN_ID,
            source_report_definition = report,
            s3_conn_id = S3_CONN_ID,
            s3_bucket = S3_BUCKET,
            s3_key = 'GoogleAds/{0}/{1}/{2}'.format(report['reportName'], key_path, key_name)
    )

    t1