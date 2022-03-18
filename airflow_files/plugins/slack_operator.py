from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from slack import WebClient
from slack.errors import SlackApiError


class SlackNotification(BaseOperator):
    def __init__(self, text: str, channel: str = "general", secret_name: str = "slack_token", **kwargs) -> None:
        super().__init__(**kwargs)
        self.text = text
        self.channel = channel
        self.secret_name = secret_name

    def execute(self, context):
        slack_token = Variable.get(self.secret_name)
        client = WebClient(token=slack_token)
        try:
            client.chat_postMessage(channel=self.channel, text=self.text)
        except SlackApiError as e:
            assert e.response["error"]
