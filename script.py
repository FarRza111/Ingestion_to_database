import pyodbc as odbc
import pandas as pd
import pyodbc as odbc
import datetime
import logging
import warnings
warnings.filterwarnings('ignore')

connection = odbc.connect('DSN=impala 64bit', autocommit=True)

absolutepath = 'C:\\Desktop\\myworkspace'
querypath = 'C:\\Desktop\\myworkspace'\\queries\\'


with open(querypath + 'accountup.txt', 'r') as reader:
    account_query = reader.read()

account_df = pd.read_sql(account_query, connection)

with open(querypath + 'TestingCustomer.txt', 'r') as reader:
    customers_query = reader.read()
customers_df = pd.read_sql(customers_query, connection)


customers_df.rename(columns={"'TESTRULE'": "Rule_ids"}, inplace=True)
account_df.rename(columns={"'TESTRULE'": "Rule_ids"}, inplace=True)

df_all = pd.concat([customers_df,account_df], ignore_index=True)
df_all["version_date"] = pd.to_datetime(df_all["version_date"])
df_all["percentage"] = df_all["percentage"].astype(int)


# Configure logging
logging.basicConfig(filename='script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    connection = odbc.connect('DSN=impala 64bit', autocommit=True)
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM DB.DQDATA WHERE version_date = ?", (today,))
    existing_records = cursor.fetchone()[0]

    if existing_records > 0:
        message = f"Today's data already exists in the table. No new records inserted on {datetime.datetime.now()}"
        logging.info(message)
        print(message)
    else:
        data = [(row['Rule_ids'], row['allrecord'], row['successful'], today, row['failed'], row['percentage'], row['status']) for index, row in df_all.iterrows()]

        # Insert the data into the table
        insert_query = """
        INSERT INTO DB.DQDATA (rule_id, allrecord, successful, version_date, failed, percentage, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_query, data)

        message = f"Rule table updated with {len(data)} records on {datetime.datetime.now()}"
        logging.info(message)
        print(message)

    cursor.close()
    connection.close()

except Exception as e:
    error_message = f"An error occurred: {str(e)}"
    logging.error(error_message)
    print(error_message)
