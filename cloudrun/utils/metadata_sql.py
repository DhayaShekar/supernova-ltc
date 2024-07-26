import sqlalchemy
from utils.postgres_utils import run_insert_query
from datetime import datetime
import json

def upload_metadata(db: sqlalchemy.engine.base.Engine, data):
    # For Tenant Master
    TenantId = 1
    TenantName = data['TenantName']

    # For Metadata details Table
    MetadataDetailId = 1
    MetadataJson = json.dumps(data['MetadataJson'])

    # Timestamps
    OnboardedDate = str(datetime.now())
    IsActive = 1
    CreatedDate = str(datetime.now())
    UpdatedDate = str(datetime.now())

    tenant_master_query = f"""
        INSERT INTO snconfig."Tenant_Master" ("TenantId","TenantName","OnboardedDate","IsActive","CreatedDate","UpdatedDate") 
        values ({TenantId},'{TenantName}','{OnboardedDate}',{IsActive},'{CreatedDate}','{UpdatedDate}')
    """

    run_insert_query(db, tenant_master_query)

    metadata_details_query = f"""
        INSERT INTO snconfig."Metadata_Details" ("MetadataDetailId","TenantId","MetadataJson","IsActive","CreatedDate","UpdatedDate") 
        values ({MetadataDetailId},{TenantId},'{MetadataJson}',{IsActive},'{CreatedDate}','{UpdatedDate}')
    """

    run_insert_query(db, metadata_details_query)

    return "Success"


def update_metadata(db: sqlalchemy.engine.base.Engine, data):
    MetadataDetailId = data['MetadataDetailId']
    MetadataJson = json.dumps(data['MetadataJson'])
    UpdatedDate = str(datetime.now())
    metadata_details_query = f"""
        UPDATE snconfig."Metadata_Details" SET "MetadataJson" = '{MetadataJson}' , "UpdatedDate" = '{UpdatedDate}' 
         where "MetadataDetailId" = {MetadataDetailId}
"""
    run_insert_query(db, metadata_details_query)


def delete_metadata():
    pass
