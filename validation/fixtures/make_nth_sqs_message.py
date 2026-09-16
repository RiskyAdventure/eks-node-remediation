"""Build a canonical aws.health scheduledChange message body for direct SQS injection.

NTH's SQS monitor skips events whose `source` is not aws.health, so EventBridge
synthetic sources cannot exercise NTH. Instead send this body straight to the NTH
queue in an isolated staging cluster:
  aws sqs send-message --queue-url QUEUE_URL --message-body file://build/nth-sqs-message.json
affectedEntities[].entityValue is a bare instance ID, matching NTH's parser.
"""
import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("instance_id")
parser.add_argument("--account-id", required=True)
parser.add_argument("--region", required=True)
parser.add_argument("--event-type-code", default="AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED")
parser.add_argument(
    "--scheduled-in-minutes", type=int, default=0,
    help="startTime this many minutes ahead (NTH schedules a drain at startTime - nodeTerminationGracePeriod)",
)
args = parser.parse_args()
if not args.instance_id.startswith("i-"):
    raise SystemExit("instance_id must start with i-")
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
start_time = (datetime.now(timezone.utc) + timedelta(minutes=args.scheduled_in_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
event = {
    "version": "0",
    "id": str(uuid.uuid4()),
    "detail-type": "AWS Health Event",
    "source": "aws.health",
    "account": args.account_id,
    "time": now,
    "region": args.region,
    "resources": [args.instance_id],
    "detail": {
        "eventArn": f"arn:aws:health:{args.region}::event/EC2/{args.event_type_code}/{args.event_type_code}_{uuid.uuid4().hex[:12]}",
        "service": "EC2",
        "eventTypeCode": args.event_type_code,
        "eventTypeCategory": "scheduledChange",
        "eventScopeCode": "ACCOUNT_SPECIFIC",
        "eventRegion": args.region,
        "affectedAccount": args.account_id,
        "statusCode": "open",
        "startTime": start_time,
        "affectedEntities": [{"entityValue": args.instance_id}],
    },
}
output = Path(__file__).resolve().parent / "build" / "nth-sqs-message.json"
output.parent.mkdir(exist_ok=True)
output.write_text(json.dumps(event, separators=(",", ":")) + "\n")
print(output)
