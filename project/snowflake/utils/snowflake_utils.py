import snowflake.connector

def get_snowflake_connection():
    return snowflake.connector.connect(
	user='afaf',
	password='Afaf-123456789',
	account='ifxjncd-bp60850',
	warehouse='COMPUTE_WH',
	database='PUBLIC_TRANSPORT_DB',
	schema='RAW',
	role='ACCOUNTADMIN'
    )
