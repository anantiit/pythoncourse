import boto3
import calendar
from config import config
from datetime import datetime
from StatusPage import StatusPage
from urllib.parse import urlparse
import re

NA_GET_ITEMIZATION = 0
NA_GET_RECOMMENDATIONS = 1
NA_GET_RECO_FEEDBACK = 2
NA_GET_SURVEY = 3
NA_GET_SURVEY_STATUS = 4
NA_PUT_SURVEY_UPDATE = 5
NA_GET_BILLINGDATA = 6

NA_GET_BROWSE_DAY = 7
NA_GET_BROWSE_MONTH = 8
NA_GET_BROWSE_YEAR = 9
NA_GET_STREAMS_LABELTIMESTAMPS = 10
NA_GET_WEATHER = 11
NA_POST_SUBSCRIBE = 12
NA_POST_UNSUBSCRIBE = 13
NA_GET_PII_USER = 14

PATTERNS = {
    NA_GET_ITEMIZATION: {
        'method': 'GET',
        'path': '^/v2.0/users/[\w-]+/endpoints/[\w-]+/itemizationDetails$'
    },
    NA_GET_RECOMMENDATIONS: {
        'method': 'GET',
        'path': '^/2.0/recommendations/users/[\w-]+/home/1$'
    },
    NA_GET_RECO_FEEDBACK: {
        'method': 'GET',
        'path': '^/2.0/recommendation-feedback$'
    },
    NA_GET_SURVEY: {
        'method': 'GET',
        'path': '^/v2.0/users/[\w-]+/homes/1/survey$'
    },
    NA_GET_SURVEY_STATUS: {
        'method': 'GET',
        'path': '^/v2.0/users/[\w-]+/homes/1/survey/status$'
    },
    NA_PUT_SURVEY_UPDATE: {
        'method': 'PUT',
        'path': '^/v2.0/users/[\w-]+/homes/1/survey$'
    },
    NA_GET_BILLINGDATA: {
        'method': 'GET',
        'path': '/billingdata/users/[\w-]+/homes/1/billingcycles'
    },

    NA_GET_BROWSE_DAY: {
        'method': 'GET',
        'path': '^/2.1/users/[\w-]+/homes/1/browse$',
        'query': 'mode=day'
    },
    NA_GET_BROWSE_MONTH: {
        'method': 'GET',
        'path': '^/2.1/users/[\w-]+/homes/1/browse$',
        'query': 'mode=month'
    },
    NA_GET_BROWSE_YEAR: {
        'method': 'GET',
        'path': '^/2.1/users/[\w-]+/homes/1/browse$',
        'query': 'mode=year'
    },
    NA_GET_STREAMS_LABELTIMESTAMPS: {
        'method': 'GET',
        'path': '^/streams/users/[\w-]+/homes/1/labelTimestamps/ELECTRIC/GreenButton$'
    },
    NA_GET_WEATHER: {
        'method': 'GET',
        'path': '^/weather/US/[\w-]+/data$'
    },
    NA_POST_SUBSCRIBE: {
        'method': 'POST',
        'path': '^/v2.1/notifications/users/[\w-]+/preferences/Email/subscribe$'
    },
    NA_POST_UNSUBSCRIBE: {
        'method': 'POST',
        'path': '^/v2.1/notifications/users/[\w-]+/preferences/Email/unsubscribe$'
    },
    NA_GET_PII_USER: {
        'method': 'GET',
        'path': '^/v2.0/pii/users/[\w-]+$'
    },
}

METRIC_DATA = {
    NA_GET_ITEMIZATION: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_ITEMIZATION',
        'sp_publish': False
    },
    NA_GET_RECOMMENDATIONS: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_RECOMMENDATIONS',
        'sp_publish': False
    },
    NA_GET_RECO_FEEDBACK: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_RECO_FEEDBACK',
        'sp_publish': False
    },
    NA_GET_SURVEY: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_SURVEY',
        'sp_publish': False
    },
    NA_GET_SURVEY_STATUS: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_SURVEY_STATUS',
        'sp_publish': False
    },
    NA_PUT_SURVEY_UPDATE: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_PUT_SURVEY_UPDATE',
        'sp_publish': False
    },
    NA_GET_BILLINGDATA: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NAApiGBBilling',
        'sp_publish': False
    },

    NA_GET_BROWSE_DAY: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NAApiGBDay',
        'sp_publish': False
    },
    NA_GET_BROWSE_MONTH: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NAApiGBMonth',
        'sp_publish': True,
        'sp_metric_id': '14xktb0y4lyc'
    },
    NA_GET_BROWSE_YEAR: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NAApiGBYear',
        'sp_publish': False
    },
    NA_GET_STREAMS_LABELTIMESTAMPS: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_STREAMS_LABELTIMESTAMPS',
        'sp_publish': False
    },
    NA_GET_WEATHER: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_WEATHER',
        'sp_publish': False
    },
    NA_POST_SUBSCRIBE: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_POST_SUBSCRIBE',
        'sp_publish': False
    },
    NA_POST_UNSUBSCRIBE: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_POST_UNSUBSCRIBE',
        'sp_publish': False
    },
    NA_GET_PII_USER: {
        'cw_metric_namespace': 'MyCustomMetrics',
        'cw_metric_name': 'NA_GET_PII_USER',
        'sp_publish': False
    },
}

status_types = [
    '2xx', '3xx', '4xx', '5xx',
]


def dt_to_ts(dt):
    return int(calendar.timegm(dt.timetuple()))


def batch_initialize(val):
    def by_response_codes(val):
        return {
            '2xx': val,
            '3xx': val,
            '4xx': val,
            '5xx': val,
        }

    return {
        NA_GET_ITEMIZATION: by_response_codes(val),
        NA_GET_RECOMMENDATIONS: by_response_codes(val),
        NA_GET_RECO_FEEDBACK: by_response_codes(val),
        NA_GET_SURVEY: by_response_codes(val),
        NA_GET_SURVEY_STATUS: by_response_codes(val),
        NA_PUT_SURVEY_UPDATE: by_response_codes(val),
        NA_GET_BILLINGDATA: by_response_codes(val),

        NA_GET_BROWSE_DAY: by_response_codes(val),
        NA_GET_BROWSE_MONTH: by_response_codes(val),
        NA_GET_BROWSE_YEAR: by_response_codes(val),
        NA_GET_STREAMS_LABELTIMESTAMPS: by_response_codes(val),
        NA_GET_WEATHER: by_response_codes(val),
        NA_POST_SUBSCRIBE: by_response_codes(val),
        NA_POST_UNSUBSCRIBE: by_response_codes(val),
        NA_GET_PII_USER: by_response_codes(val),
    }


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    print(event)
    for index, record in enumerate(event['Records']):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(bucket)

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        logs = content.split('\n')

        if not logs:
            continue

        last_time = batch_initialize(None)
        total_time = batch_initialize(0.0)
        number_of_logs = batch_initialize(0)
        avg_time = batch_initialize(0.0)

        for log_slug in logs:
            if not log_slug:
                continue

            log = log_slug.split(' ')

            method = log[11][1:]
            url = log[12]
            path = urlparse(url).path
            query = urlparse(url).query

            for log_type in PATTERNS.keys():
                if method == PATTERNS[log_type]['method'] and re.search(PATTERNS[log_type]['path'], path):
                    if 'query' in PATTERNS[log_type].keys() and PATTERNS[log_type]['query'] not in query:
                        continue
                    print("log_type = {}".format(log_type))
                    if int(log[8]) < 300:
                        status_type = '2xx'
                    elif int(log[8]) < 400:
                        status_type = '3xx'
                    elif int(log[8]) < 500:
                        status_type = '4xx'
                    else:
                        status_type = '5xx'

                    last_time[log_type][status_type] = log[0]
                    number_of_logs[log_type][status_type] += 1
                    if bucket == "elb-api-write-logs-prod-na":
                        total_time[log_type][status_type] += float(log[4]) + float(log[5]) + float(log[6])
                    else:
                        total_time[log_type][status_type] += float(log[4]) + float(log[5]) + float(log[6]) / 1000000

        for log_type in PATTERNS.keys():
            for status_type in status_types:
                if number_of_logs[log_type][status_type]:
                    avg_time[log_type][status_type] = total_time[log_type][status_type] / number_of_logs[log_type][
                        status_type]
                    last_time[log_type][status_type] = datetime.strptime(last_time[log_type][status_type],
                                                                         '%Y-%m-%dT%H:%M:%S.%fZ')
                    print("type - {} total time - {} number of logs - {} last time - {}\n".format(log_type,
                                                                                                  total_time[log_type][
                                                                                                      status_type],
                                                                                                  number_of_logs[
                                                                                                      log_type][
                                                                                                      status_type],
                                                                                                  last_time[log_type][
                                                                                                      status_type]))

        sp_data = {}
        sp = StatusPage(config['statuspage_api_key'], config['statuspage_page_id'])
        client = boto3.client('cloudwatch')

        for log_type in PATTERNS.keys():
            for status_type in status_types:
                if number_of_logs[log_type][status_type]:
                    metric_data = METRIC_DATA[log_type]

                    if bucket == "elb-api-write-logs-prod-na":
                        metricname = metric_data['cw_metric_name'] + '-internal'
                    else:
                        metricname = metric_data['cw_metric_name']

                    client.put_metric_data(
                        Namespace=metric_data['cw_metric_namespace'],
                        MetricData=[{
                            'MetricName': metricname,
                            'Dimensions': [
                                {
                                    'Name': 'StatusCode',
                                    'Value': status_type
                                },
                                {
                                    'Name': 'Type',
                                    'Value': 'ResponseTime'
                                }
                            ],
                            'Timestamp': last_time[log_type][status_type],
                            'Value': avg_time[log_type][status_type]
                        }]
                    )

                    client.put_metric_data(
                        Namespace=metric_data['cw_metric_namespace'],
                        MetricData=[{
                            'MetricName': metricname,
                            'Dimensions': [
                                {
                                    'Name': 'StatusCode',
                                    'Value': status_type
                                },
                                {
                                    'Name': 'Type',
                                    'Value': 'NumberOfRequests'
                                }
                            ],
                            'Timestamp': last_time[log_type][status_type],
                            'Value': number_of_logs[log_type][status_type]
                        }]
                    )

                    if bucket == "elb-api-read-logs-prod-na":
                        if metric_data['sp_publish'] and status_type == '2xx':
                            sp_data[metric_data['sp_metric_id']] = {
                                dt_to_ts(last_time[log_type][status_type]): avg_time[log_type][status_type]
                            }

        for log_type in PATTERNS.keys():
            for status_type in status_types:
                if number_of_logs[log_type][status_type] >= 3:
                    metric_data = METRIC_DATA[log_type]

                    if bucket == "elb-api-write-logs-prod-na":
                        metricname = metric_data['cw_metric_name'] + '-internal'
                        continue
                    else:
                        metricname = metric_data['cw_metric_name'] + '-filtered'

                    client.put_metric_data(
                        Namespace=metric_data['cw_metric_namespace'],
                        MetricData=[{
                            'MetricName': metricname,
                            'Dimensions': [
                                {
                                    'Name': 'StatusCode',
                                    'Value': status_type
                                },
                                {
                                    'Name': 'Type',
                                    'Value': 'ResponseTime'
                                }
                            ],
                            'Timestamp': last_time[log_type][status_type],
                            'Value': avg_time[log_type][status_type]
                        }]
                    )

                    client.put_metric_data(
                        Namespace=metric_data['cw_metric_namespace'],
                        MetricData=[{
                            'MetricName': metricname,
                            'Dimensions': [
                                {
                                    'Name': 'StatusCode',
                                    'Value': status_type
                                },
                                {
                                    'Name': 'Type',
                                    'Value': 'NumberOfRequests'
                                }
                            ],
                            'Timestamp': last_time[log_type][status_type],
                            'Value': number_of_logs[log_type][status_type]
                        }]
                    )

        if sp_data:
            print(sp.push_metrics(sp_data))
