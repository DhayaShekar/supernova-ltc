import json

def parse_config(ip_json):
    #print(ip_json, flush=True)
    jsonDict = json.loads(ip_json)
    print(jsonDict, flush=True)
    #with open('ip_json') as config_file:
    #    data = json.load(config_file)
    print(jsonDict['TargetDetails']['drivername'], flush=True)
    return jsonDict['TargetDetails']['TenantConfigId']

parse_config('{"TenantConfigId": "1234", "MetadataDetailId": 30, "TargetDetails": {"drivername":"postgresql+pg8000","username":"db_user","password":"db_pass","port":"db_port","database":"db_name"}}')
#TenantConfigId,TargetDetails,MetadataDetailId

