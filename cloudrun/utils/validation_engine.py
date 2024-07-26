# Special Characters
# Len < 30 Characters
# Case matching
# Schema Check

import datetime
from utils.postgres_utils import run_insert_query, get_results_query
from utils.mail_generator import send_mail
from prettytable import PrettyTable

def special_character_check(source):
    x = ["!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "+", "?", "=", "<", ">", "/"]
    for each_word in source:
        # print(each_word)
        for each_character in each_word:
            # print(each_character)
            if each_character in x:
                return False
    return True


def length_check(source):
    for each_word in source:
        if len(each_word) > 30:
            return False

    return True


def character_matching(source, target):
    s = []
    t = []
    for each_word in source:
        s.append(each_word.lower())
    for each_word in target:
        t.append(each_word.lower())

    for each_word in s:
        if each_word not in t:
            return False

    return True


def schema_check(source, target):
    s = {}
    t = {}
    tag = True
    for each_word in source:
        s[each_word.lower()] = source[each_word]
    for each_word in target:
        t[each_word.lower()] = target[each_word]

    failed = {}
    for each_word in s:
        if s[each_word] != t[each_word]:
            failed[each_word] = [s[each_word], t[each_word]]
            tag = False

    return failed, tag


def insert_into_validation_table(db, failed, tag):
    ValidationResultId = 1
    RunId = 1
    TenantConfigId = 1
    IsActive = 1
    CreatedDate = str(datetime.datetime.now())
    UpdatedDate = str(datetime.datetime.now())

    for each_failure in failed:
        query = f"""
            INSERT INTO snconfig."Validation_Results" ("ValidationResultId","RunId","TenantConfigId","TenantType","ColumnName","DataType","IsActive","CreatedDate","UpdatedDate") 
            values ({ValidationResultId}, {RunId}, {TenantConfigId}, 'Source','{each_failure}', '{failed[each_failure][0]}', {IsActive} ,'{CreatedDate}', '{UpdatedDate}')
        """
        run_insert_query(db, query)
        target_validation_result_id = 2
        query = f"""
                    INSERT INTO snconfig."Validation_Results" ("ValidationResultId","RunId","TenantConfigId","TenantType","ColumnName","DataType","IsActive","CreatedDate","UpdatedDate") 
                    values ({target_validation_result_id}, {RunId}, {TenantConfigId}, 'Target','{each_failure}', '{failed[each_failure][1]}', {IsActive} ,'{CreatedDate}', '{UpdatedDate}')
                """
        run_insert_query(db, query)

    RunstatusId = 1
    if str(tag) == "True":
        RunstatusId = 1
    else:
        RunstatusId = 2

    RunStartDate = str(datetime.datetime.now())
    RunEndDate = str(datetime.datetime.now())
    run_history_query = f"""
        INSERT INTO snconfig."Run_History" ("RunId", "TenantConfigId" ,"RunStartDate" , "RunstatusId", "RunEndDate") 
        values ({RunId}, {TenantConfigId} , '{RunStartDate}' ,{RunstatusId} , '{RunEndDate}')
    """

    run_insert_query(db, run_history_query)


def validate(db, source_metadata, target_metadata):
    tag = True
    # if special_character_check(source_metadata) and length_check(source_metadata) and character_matching(
    #         source_metadata, target_metadata):
    #     tag = True
    # else:
    #     tag = False

    failed, tag = schema_check(source_metadata, target_metadata)

    insert_into_validation_table(db, failed, tag)

    results_query = f"""
    select "TenantName","TenantType","ColumnName","DataType",current_date "OccuringSince" from snconfig."Run_History"
    left join 
    snconfig."Validation_Results"
    on "Run_History"."RunId" = "Validation_Results"."RunId"
    inner JOIN snconfig."Tenant_Configuration"
    on "Tenant_Configuration"."ConfigId" = "Validation_Results"."TenantConfigId"
    inner join snconfig."Tenant_Master"
    on "Tenant_Master"."TenantId"= "Tenant_Configuration"."TenantId"
    inner join snconfig."Format_Master"
    on "Format_Master"."FormatId" = "Tenant_Configuration"."SourceFormatId"
    inner join snconfig."MetadataSource_Master"
    on "MetadataSource_Master"."MetadataSourceId" = "Tenant_Configuration"."MetadataSourceId"
    """

    results = get_results_query(db, results_query)

    mail = "divyan.8726@gmail.com"

    x = PrettyTable()

    x.field_names = ["TenantName", "TenantType", "ColumnName", "DataType", "OccuringSince"]
    for r in results:
        # print(list(r))
        x.add_row(list(r))

    print(x)
    message = f"Run validation {tag} and results are \n {x}"

    send_mail(mail, message)

    return failed



