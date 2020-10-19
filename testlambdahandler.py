import json
import random
from unittest import TestCase

from lambda_function import upload_response_list_to_s3, read_s3_file, analyze_w_last100, read_last_100


class TestLambdaHandler(TestCase):

    def test_upload_download_list_to_s3(self):

        list_responses = []
        random_int = random.randint(1, 10)
        for i in range(random_int):
            response_example = self.generate_response()
            response_example['ResponseMetadata']['id'] = i
            list_responses.append(response_example)
        print(list_responses)
        print('typelocal', type(list_responses))

        upload_response_list_to_s3(list_responses)

        s3_for_check = json.loads(read_s3_file())
        print(len(s3_for_check))

        need_length = 3
        if len(s3_for_check) < need_length:
            read_last_100(need_length, 100)

        for i in range(random_int):
            self.assertTrue(list_responses[i]['ResponseMetadata']['id'] == s3_for_check[i]['ResponseMetadata']['id'])
        self.assertTrue(len(s3_for_check) == len(list_responses))

    def test_read_last_100(self):
        list_of_prev_responses = self.generate_response_100()
        print('list_of_prev100', list_of_prev_responses)
        self.assertEqual(100, len(read_last_100(list_of_prev_responses, need_length=100)))

    @staticmethod
    def single_response():
        return {'ResultsByTime': [{'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                                   'Total': {'NetAmortizedCost': {'Amount': '1000.00',
                                                                  'Unit': 'USD'}}, 'Groups': [],
                                   'Estimated': True}],
                'ResponseMetadata': {'RequestId': '2885e189-2204-4911-aaac-1026b354f0cb',
                                     'HTTPStatusCode': 200, 'HTTPHeaders':
                                         {'date': 'Mon, 12 Oct 2020 18:41:44 GMT', 'content-type':
                                             'application/x-amz-json-1.1', 'content-length': '176',
                                          'connection': 'keep-alive', 'x-amzn-requestid':
                                              '2885e189-2204-4911-aaac-1026b354f0cb', 'cache-control':
                                              'no-cache'}, 'RetryAttempts': 0}}

    @staticmethod
    def generate_response():
        return {'ResultsByTime': [{'TimePeriod': {'Start': '2020-09-01', 'End': '2020-09-02'},
                                   'Total': {'NetAmortizedCost': {'Amount': '1950.3269595416', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-02', 'End': '2020-09-03'},
                                   'Total': {'NetAmortizedCost': {'Amount': '343.2447618466', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-03', 'End': '2020-09-04'},
                                   'Total': {'NetAmortizedCost': {'Amount': '377.4367798476', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-04', 'End': '2020-09-05'},
                                   'Total': {'NetAmortizedCost': {'Amount': '328.9741023788', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-05', 'End': '2020-09-06'},
                                   'Total': {'NetAmortizedCost': {'Amount': '300.582115085', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-06', 'End': '2020-09-07'},
                                   'Total': {'NetAmortizedCost': {'Amount': '348.1442765254', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-07', 'End': '2020-09-08'},
                                   'Total': {'NetAmortizedCost': {'Amount': '311.3157645332', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-08', 'End': '2020-09-09'},
                                   'Total': {'NetAmortizedCost': {'Amount': '328.4935501509', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-09', 'End': '2020-09-10'},
                                   'Total': {'NetAmortizedCost': {'Amount': '339.0183537492', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-10', 'End': '2020-09-11'},
                                   'Total': {'NetAmortizedCost': {'Amount': '345.4777755757', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-11', 'End': '2020-09-12'},
                                   'Total': {'NetAmortizedCost': {'Amount': '338.8645989463', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-12', 'End': '2020-09-13'},
                                   'Total': {'NetAmortizedCost': {'Amount': '304.8868899379', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-13', 'End': '2020-09-14'},
                                   'Total': {'NetAmortizedCost': {'Amount': '302.4413338508', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-14', 'End': '2020-09-15'},
                                   'Total': {'NetAmortizedCost': {'Amount': '334.6509754296', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-15', 'End': '2020-09-16'},
                                   'Total': {'NetAmortizedCost': {'Amount': '338.4812687858', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-16', 'End': '2020-09-17'},
                                   'Total': {'NetAmortizedCost': {'Amount': '348.1924434902', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-17', 'End': '2020-09-18'},
                                   'Total': {'NetAmortizedCost': {'Amount': '344.5319297503', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-18', 'End': '2020-09-19'},
                                   'Total': {'NetAmortizedCost': {'Amount': '431.6950435201', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-19', 'End': '2020-09-20'},
                                   'Total': {'NetAmortizedCost': {'Amount': '372.4132623298', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-20', 'End': '2020-09-21'},
                                   'Total': {'NetAmortizedCost': {'Amount': '324.4482360677', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-21', 'End': '2020-09-22'},
                                   'Total': {'NetAmortizedCost': {'Amount': '363.6235230826', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-22', 'End': '2020-09-23'},
                                   'Total': {'NetAmortizedCost': {'Amount': '331.7009511894', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-23', 'End': '2020-09-24'},
                                   'Total': {'NetAmortizedCost': {'Amount': '330.0132424903', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-24', 'End': '2020-09-25'},
                                   'Total': {'NetAmortizedCost': {'Amount': '337.0115478977', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-25', 'End': '2020-09-26'},
                                   'Total': {'NetAmortizedCost': {'Amount': '347.7461445155', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-26', 'End': '2020-09-27'},
                                   'Total': {'NetAmortizedCost': {'Amount': '310.5012398675', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-27', 'End': '2020-09-28'},
                                   'Total': {'NetAmortizedCost': {'Amount': '291.5453049245', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-28', 'End': '2020-09-29'},
                                   'Total': {'NetAmortizedCost': {'Amount': '338.0658190107', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-29', 'End': '2020-09-30'},
                                   'Total': {'NetAmortizedCost': {'Amount': '333.070476826', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False},
                                  {'TimePeriod': {'Start': '2020-09-30', 'End': '2020-10-01'},
                                   'Total': {'NetAmortizedCost': {'Amount': '339.8734395843', 'Unit': 'USD'}},
                                   'Groups': [], 'Estimated': False}],
                'ResponseMetadata': {'RequestId': '34fefc34-5415-4efc-a6a9-827767ea4097', 'HTTPStatusCode': 200,
                                     'HTTPHeaders': {'date': 'Wed, 14 Oct 2020 12:13:04 GMT',
                                                     'content-type': 'application/x-amz-json-1.1',
                                                     'content-length': '4728', 'connection': 'keep-alive',
                                                     'x-amzn-requestid': '34fefc34-5415-4efc-a6a9-827767ea4097',
                                                     'cache-control': 'no-cache'}, 'RetryAttempts': 0}}

    @staticmethod
    def generate_response_100():
        return [{'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                 'Total': {'NetAmortizedCost': {'Amount': '1001.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True},
                {'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                 'Total': {'NetAmortizedCost': {'Amount': '1002.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True},
                {'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                 'Total': {'NetAmortizedCost': {'Amount': '1003.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True},
                {'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                 'Total': {'NetAmortizedCost': {'Amount': '1004.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True},
                {'TimePeriod': {'Start': '2020-10-15', 'End': '2020-10-16'},
                 'Total': {'NetAmortizedCost': {'Amount': '1005.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True},
                {'TimePeriod': {'Start': '2020-10-01', 'End': '2020-10-02'},
                 'Total': {'NetAmortizedCost': {'Amount': '1800.00',
                                                'Unit': 'USD'}}, 'Groups': [],
                 'Estimated': True}]
