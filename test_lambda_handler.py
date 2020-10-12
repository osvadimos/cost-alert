from unittest import TestCase
from lambda_function import lambda_handler


class TestLambda_handler(TestCase):


    def test_lambda_handler(self):
        event = {"arn": "arn:aws:lambda:eu-west-1:222222222:function:lambda_handler"}
        context = object()
        response = lambda_handler(event=event, context=context)
        self.fail()
