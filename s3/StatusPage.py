from urllib import parse as url_parser
import requests


class StatusPage:
    def __init__(self, api_key, page_id):
        self.page_id = page_id
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "OAuth " + self.api_key
        }

    def push_metrics(self, data):
        # data = {
        # 	'metric_id1': {
        # 		'timestamp1': 'value1',
        # 		'timestamp2': 'value2',
        # 	},
        # 	'metric_id2': {
        # 		'timestamp3': 'value3',
        # 		'timestamp4': 'value4'
        # 	}
        # }

        request_url = "https://api.statuspage.io/v1/pages/{}/metrics/data.json".format(self.page_id)
        payload = ""

        for metric_id, datapoints in data.items():
            for timestamp, value in datapoints.items():
                data_metric = "data[{}]".format(metric_id)
                payload += url_parser.urlencode({
                    data_metric + '[][timestamp]': timestamp,
                    data_metric + '[][value]': value
                }) + "&"

        r = requests.post(request_url, data=payload, headers=self.headers)
        return r.text

    def update_component(self, component_id, status):
        request_url = "https://api.statuspage.io/v1/pages/{}/components/{}.json".format(self.page_id, component_id)
        payload = url_parser.urlencode({"component[status]": status})

        r = requests.post(request_url, data=payload, headers=self.headers)
        return r.text
