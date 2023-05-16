import cx_Oracle
import psycopg2


#Scheduling

# Connect to the Oracle database
orcl = cx_Oracle.connect(
    user="HMA_DM_SODAS",
    password="H$Ma_mnb7898",
    dsn="//nasisexadb-scan:1521/ddwh"
)

# Connect to the PostgreSQL database(CMT)
pgdb = psycopg2.connect(
    host="ec2-50-19-178-92.compute-1.amazonaws.com",
    user="ucvv2qbgdlvde3",
    password="p09577fa3bec788eb12beeed4b709cbdbc27eb2caa27e9fb9d646b3f70b3d37b1",
    database="de2uq7s3g08a9g"
)    
    
# Create a cursor to perform operations on the Oracle database
orclcursor = orcl.cursor()

# Create a cursor to perform operations on the PostgreSQL database
pgcursor = pgdb.cursor()

# Execute an Oracle query to retrieve the data
orclcursor.execute("select * from SODAS_ALERT_SUMMARY")

# Fetch the result of the Oracle query
oracle_data = orclcursor.fetchall()

pgsql = '''
CREATE TABLE IF NOT EXISTS einstein.cmt_sodas_alert (
        alert_id        character varying(50)    NOT NULL, -- 사용자아이디
        alert_type      character varying(250)    null,    -- 사용자이름
        alert_gen_date  date,							   -- alert  발생일
        alert_dis_date  date,							   -- alert  disposition date
        sodas_dis_date  date,							   -- sodas  disposition date
        alert_status    character varying(100)    null,    -- alert 상태
        alert_dis_decision     character varying(100)    null,    -- alert disposition decision
        alert_owner      character varying(100)    null,    -- alert owner
        first_event_source character varying(250)    null,    -- 사용자이름
        alert_risk_source numeric
)
'''
pgcursor.execute(pgsql)

# Loop through the result and insert each row into the PostgreSQL table
for row in oracle_data:
    pgcursor.execute("INSERT INTO einstein.cmt_sodas_alert VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

# Commit the changes to the PostgreSQL database
pgdb.commit()

# Close the cursors and databases
orclcursor.close()
pgcursor.close()
orcl.close()
pgdb.close()
print("Finished.")

