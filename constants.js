OK = 'OK';
ERROR = 'ERROR';
FORWARD_TO_FIREHOSE_STREAM = "ForwardToFirehoseStream";
DDB_SERVICE_NAME = "aws:dynamodb";
KINESIS_SERVICE_NAME = "aws:kinesis";
FIREHOSE_MAX_BATCH_COUNT = 500;
// firehose max PutRecordBatch size 4MB
FIREHOSE_MAX_BATCH_BYTES = 4 * 1024 * 1024;
MAX_RETRY_ON_FAILED_PUT = process.env['MAX_RETRY_ON_FAILED_PUT'] || 3;
RETRY_INTERVAL_MS = process.env['RETRY_INTERVAL_MS'] || 300;
targetEncoding = "utf8";
ACCOUNT_ID_FIELD = "account_id";
MYRA_REALM = process.env["MYRA_REALM"] || "dev";
DELIVERY_STREAM_PREFIX = "firehose-stream-" + MYRA_REALM + "-";
DEFAULT_DELIVERY_STREAM_NAME = DELIVERY_STREAM_PREFIX + "no-account-id";
