from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)  # This enables templating for s3_key

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 copy_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info(f"Staging data from S3 to Redshift table {self.table}")

        # Get AWS credentials
        s3 = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = s3.get_credentials(context=context)

        # Render the S3 key with context (supports templating)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Build the COPY SQL command
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            """

        if credentials.token:
            copy_sql += f"\n    SESSION_TOKEN '{credentials.token}'"

        copy_sql += f"""
            FORMAT AS JSON '{self.json_path}'
            {self.copy_options}
            ;
        """

        self.log.info(f"Executing COPY command: {copy_sql}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(copy_sql)

        self.log.info(f"Staging to {self.table} completed.")

