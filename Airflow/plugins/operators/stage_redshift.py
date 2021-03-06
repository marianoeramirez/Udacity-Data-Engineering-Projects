from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 aws_credential_id: str = "",
                 table_name: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 log_json_file: str = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()

        base_copy_query = " COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' region 'us-west-2' FORMAT AS json '{}';"

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        if self.log_json_file != "":
            self.log_json_file = f"s3://{self.s3_bucket}/{self.log_json_file}"
            copy_query = base_copy_query.format(self.table_name, s3_path, credentials.access_key,
                                                credentials.secret_key, self.log_json_file)
        else:
            copy_query = base_copy_query.format(self.table_name, s3_path, credentials.access_key,
                                                credentials.secret_key, 'auto')

        self.log.info(f"Running copy: {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table_name} ready!")
