# Special Characters
# Len < 30 Characters
# Case matching
# Schema Check

from datetime import datetime
from utils.postgres_utils import run_insert_query

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
    CreatedDate = str(datetime.now())
    UpdatedDate = str(datetime.now())

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

    RunStartDate = str(datetime.now())
    RunEndDate = str(datetime.now())
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

    return failed



