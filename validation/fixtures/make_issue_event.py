"""Build a synthetic AWS Health EC2 *issue* EventBridge entry for staging injection.

The entry uses a custom `source` so it can only match the SyntheticIssueRule; the
adapter must also list that source in ALLOWED_SOURCES. The detail mirrors the real
AWS Health schema shape (bare instance IDs in affectedEntities/resources, eventArn,
eventRegion, affectedAccount). Put the event with:
  aws events put-events --entries file://build/eventbridge-entry.json
"""
import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("instance_ids", nargs="+", help="one or more i-... instance IDs")
parser.add_argument("--account-id", required=True)
parser.add_argument("--region", required=True)
parser.add_argument("--source", default="custom.node-remediation.synthetic")
parser.add_argument("--event-type-code", default="AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED")
parser.add_argument("--event-id", default=None, help="stable Health event id; random when omitted")
parser.add_argument("--output", default="eventbridge-entry.json")
args = parser.parse_args()
for instance in args.instance_ids:
    if not instance.startswith("i-"):
        raise SystemExit(f"instance id must start with i-: {instance}")

event_id = args.event_id or uuid.uuid4().hex[:16]
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
detail = {
    "eventArn": f"arn:aws:health:{args.region}::event/EC2/{args.event_type_code}/{args.event_type_code}_{event_id}",
    "service": "EC2",
    "eventTypeCode": args.event_type_code,
    "eventTypeCategory": "issue",
    "eventScopeCode": "ACCOUNT_SPECIFIC",
    "eventRegion": args.region,
    "affectedAccount": args.account_id,
    "statusCode": "open",
    "startTime": now,
    "lastUpdatedTime": now,
    "eventDescription": [{"language": "en_US", "latestDescription": "Synthetic staging event"}],
    "affectedEntities": [{"entityValue": instance, "status": "IMPAIRED"} for instance in args.instance_ids],
    "page": "1",
    "totalPages": "1",
}
entry = [{
    "Source": args.source,
    "DetailType": "AWS Health Event",
    "Detail": json.dumps(detail, separators=(",", ":")),
    "Resources": list(args.instance_ids),
    "EventBusName": "default",
}]
build = Path(__file__).resolve().parent / "build"
build.mkdir(exist_ok=True)
output = build / args.output
output.write_text(json.dumps(entry, indent=2) + "\n")
print(output)
