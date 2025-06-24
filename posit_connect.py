import pandas as pd
import numpy as np
import os
import sqlalchemy as sa #sqlalchemy 1.4.39 for python3.9
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy import text
import pyodbc # need to use pyodbc to create and use temp tables, sqlalchemy cannot
from dotenv import load_dotenv
from pathlib import Path
import getpass

load_dotenv("../.env")
SERVER_QA=os.getenv("SERVER_QA")
USERNAME=os.getenv("USERNAME1")
PASSWORD=os.getenv("PASSWORD1")

print(pyodbc.drivers())
ODBC_DRIVER_VERSION = "ODBC Driver 18 for SQL Server"
assert ODBC_DRIVER_VERSION in pyodbc.drivers()
conn_string_qa3 = f"Driver={{{ODBC_DRIVER_VERSION}}};Server={SERVER_QA};Database=WDRS;UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=10;ApplicationIntent=ReadOnly"

with pyodbc.connect(conn_string_qa3) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT 1 from IDS_CASE")
    for row in cursor.fetchall():
        print(row)

instance = 'QA'

qa_connection_string = f"DRIVER={{{ODBC_DRIVER_VERSION}}};SERVER={SERVER_QA};DATABASE=WDRS;UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=10;ApplicationIntent=ReadOnly"
# connection_string = f"DRIVER={{{ODBC_DRIVER_VERSION}}};SERVER={SERVER_PROD};DATABASE=WDRS;UID={EMAIL};PWD={PASSWORD};Authentication=ActiveDirectoryPassword;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=0;ApplicationIntent=ReadOnly"

# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
qa_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": qa_connection_string})

# use sqlalchemy with pd.read_sql_query when possible because results easily go into pandas data frame
if instance == 'QA':
    engine = create_engine(qa_connection_url)
    days = '-60' # go back 60 days in QA to get enough data
elif instance == 'PROD':
    engine = create_engine(connection_url)
    days = '-4'

# Pull new cases from WDRS
SQL_cases_no_labs = ("SELECT DISTINCT "
             "m.CASE_ID AS case_id, "
             "m.CODE AS code, "
             "m.FIRST_NAME AS first_name, "
             "m.MIDDLE_NAME AS middle_name, "
             "m.LAST_NAME AS last_name, "
             "m.DIED_ILLNESS AS died_illness, "
             "m.DOH_CASE_CLASSIFICATION_GENERAL AS doh_case_classification_general, "
             "m.INITIAL_REPORTING_SOURCE AS initial_reporting_source, "
             "m.INITIAL_REPORTING_SOURCE_ORGANIZATION_NAME_PERSON_REPORTING_CASE AS initial_reporting_source_organization_name_person_reporting_case, "
             "m.INITIAL_REPORTING_SOURCE_ORGANIZATION_TELEPHONE AS initial_reporting_source_organization_telephone, "
             "m.INITIAL_REPORTING_SOURCE_OTHER_SPECIFY AS initial_reporting_source_other_specify, "
             "m.INITIAL_REPORTING_SOURCE_OTHER_STATE AS initial_reporting_source_other_state, "
             "m.INITIAL_REPORTING_SOURCE_OTHER_LHJ AS initial_reporting_source_other_lhj, "
             "m.INITIAL_REPORTING_SOURCE_REPORTER_ORGANIZATION AS initial_reporting_source_reporter_organization, "
             "m.LHJ_NOTIFICATION_DATE AS lhj_notification_date, "
             "m.RARE_DISEASE_PUBLIC_HEALTH_SIGNIFICANCE AS rare_disease_public_health_significance, "
             "m.ACCOUNTABLE_COUNTY AS accountable_county, " 
             "m.WASHINGTON_STATE_RESIDENT AS washington_state_resident, "
             "m.BIRTH_DATE AS dob, "
             "m.DEATH_DATE AS death_date, "
             "m.SUFFIX AS suffix, "
             "m.GENDER AS gender, "
             "m.EXTERNAL_ID AS party_external_id, "
             "cp.CITY AS city, "
             "cp.COUNTY AS county, "
             "cp.POSTAL_CODE AS postal_code, "
             "cp.STATE AS state, "
             "cp.STREET1 AS street1, "
             "cp.STREET2 AS street2, "
             "cp.PRIMARY_ADDRESS AS primary_address, "
             "phone1.PHONE1 AS phone1, " 
             "phone2.PHONE2 AS phone2 "
             "FROM DD_GCD_MORI_CASE m "
             "INNER JOIN IDS_CASE cs ON cs.CASE_ID = m.CASE_ID "
             "INNER JOIN IDS_PARTICIPANT part ON part.CASE_ID = cs.UNID "
             "INNER JOIN IDS_PARTY p ON p.UNID = part.PARTY_ID "
             "INNER JOIN IDS_CONTACTPOINT cp ON cp.PARTY_ID = p.UNID "
             "LEFT JOIN ("
                 "SELECT NAME, VALUE AS 'PHONE1', PARTY_ID, ROW_NUMBER() "
                 "OVER (PARTITION BY PARTY_ID "
                 "ORDER BY ITERATION DESC) AS rn "
                 "FROM IDS_PARTY_ATTRIBUTE WHERE NAME = 'Phone') phone1 "
                 "ON phone1.PARTY_ID = p.UNID AND phone1.rn = 1 "
             "LEFT JOIN ("
                 "SELECT NAME, VALUE AS 'PHONE2', PARTY_ID, ROW_NUMBER() "
                 "OVER (PARTITION BY PARTY_ID "
                 "ORDER BY ITERATION DESC) AS rn "
                 "FROM IDS_PARTY_ATTRIBUTE WHERE NAME = 'Phone') phone2 "
                 "ON phone2.PARTY_ID = p.UNID AND phone2.rn = 2 "
             f"WHERE cs.CREATE_DATE BETWEEN DATEADD(day, {days}, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)"
             )

with engine.begin() as wdrs_conn:
        wdrs_cases = pd.read_sql_query(text(SQL_cases_no_labs), wdrs_conn)


    