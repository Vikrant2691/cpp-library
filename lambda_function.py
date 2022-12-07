import json
import urllib.parse
import boto3
import application

print('Loading function')



def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    try:
        userId = event['user_id']
        updateRecommendations(userId)
        print("UserId")
    except Exception as e:
        print(e)
        print('Error')
        raise e
