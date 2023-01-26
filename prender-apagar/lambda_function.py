import boto3


def lambda_handler(event, context):
    ec2 = boto3.client("ec2")
    rds = boto3.client("rds")
    mode = event.get("mode")
    print(event.get("mode"))

    if mode == "prender":
        try:
            ec2.start_instances(InstanceIds=["i-0797b66ef1daa1635"], DryRun=True)
            rds.start_db_instance(DBInstanceIdentifier="bne")
        except:
            print("Error prendiendo")

    elif mode == "apagar":
        try:
            rds.stop_db_instance(DBInstanceIdentifier="bne")
            ec2.stop_instances(InstanceIds=["i-0797b66ef1daa1635"])
        except:
            print("Error apagando ")

    return {
        "status": mode,
    }
