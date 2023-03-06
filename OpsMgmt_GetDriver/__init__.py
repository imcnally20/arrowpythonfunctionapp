import os
import logging
import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import pyodbc
import json
import datetime
import logging
import asyncio

async def send_message_to_topic(ops_driver_change_transactions):
    servicebus_connection_string = os.environ['AzureServiceBus']
    topic_name = "nav2driverintegration"
    ops_driver_change_dict = [driver.to_dict() for driver in ops_driver_change_transactions]
    # ops_driver_change_transactions = {"key": "value"}  # replace this with your own data


                # for driver_dict in ops_driver_change_dict:
                #     message_body = json.dumps(driver_dict)
                #     message = ServiceBusMessage(message_body)
                #     message.user_properties = {"Data": message_body}
                #     await topic_sender.send_messages(message)
    try:
        logging.info("Create context for asynchronous resources...")
        logging.info(type(json.dumps(ops_driver_change_dict)))
        servicebus_client = ServiceBusClient.from_connection_string(servicebus_connection_string)
        topic_sender = servicebus_client.get_topic_sender(topic_name)

        for driver_dict in ops_driver_change_dict:
            message_body = json.dumps(driver_dict)
            message = ServiceBusMessage(message_body)
            message.user_properties = {"Data": message_body}
            if message is not None:
                topic_sender.send_messages(message)

        # topic_sender = servicebus_client.get_topic_sender(topic_name)
        # message_body = json.dumps(ops_driver_change_dict)
        # message = ServiceBusMessage(message_body)
        # message.user_properties = {"Data": message_body}
        # if message is not None:
        #     # await topic_sender.SendAsync(message)
        #     await send_message_to_topic(message)

        # await topic_sender.send_messages(message)
        logging.info("Completed sending message to service bus.")

        # async with ServiceBusClient.from_connection_string(servicebus_connection_string) as servicebus_client:
        #     async with servicebus_client.get_topic_sender(topic_name) as topic_sender:
        #         message_body = json.dumps(ops_driver_change_dict)
        #         message = ServiceBusMessage(message_body)
        #         message.user_properties = {"Data": message_body}
        #         # message = ServiceBusMessage()
        #         # message.user_properties = {"Data": json.dumps(ops_driver_change_dict)}
        #         await topic_sender.send_messages(message)
        #         logging.info("Completed sending message to service bus.")
    except Exception as e:
        logging.error(f"Sending message to service bus failed: {e}")
        raise

class OpsDriverTransactions:
    def __init__(self
                ,ops_driver_id
                ,lastName
                ,firstName
                ,division_id
                ,homedivn
                ,pin
                ,dtype
                ,driverLicenseNo
                ,countryCode
                ,licenseIssuingState
                # ,company
                ,division_name
                ,inactiveFlag
                ,driverEmailMobile 
                ):

        self.ops_driver_id = ops_driver_id
        self.homedivn = homedivn
        self.firstName = firstName
        self.lastName = lastName
        self.division_id = division_id
        self.division_name = division_name
        self.inactiveFlag = inactiveFlag
        # self.timestamp = timestamp
        self.driverEmailMobile = driverEmailMobile
        self.dtype = dtype
        self.pin = pin
        self.driverLicenseNo = driverLicenseNo
        self.countryCode = countryCode
        self.licenseIssuingState = licenseIssuingState

    def to_dict(self):
        return {
            "ops_driver_id": self.ops_driver_id,
            "lastName": self.lastName,
            "firstName": self.firstName,
            "division_id": self.division_id,
            "homedivn": self.homedivn,
            "pin": self.pin,
            "dtype": self.dtype,
            "driverLicenseNo": self.driverLicenseNo,
            "countryCode": self.countryCode,
            "licenseIssuingState": self.licenseIssuingState,
            # "company": self.company,
            "division_name": self.division_name,
            "inactiveFlag": self.inactiveFlag,
            "driverEmailMobile": self.driverEmailMobile,
        }

# class LastDriverLUpdatedTime:
#     def __init__(self, pkey, rkey, last_details_timestamp, last_updated_timestamp):
#         self.PartitionKey = pkey
#         self.RowKey = rkey
#         self.last_details_timestamp = last_details_timestamp
#         self.last_updated_timestamp = last_updated_timestamp

async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # sb_topic_name = "nav2driverintegration"
    ops_conn_str = os.environ['opsConnection']
    azure_storage_account_str = os.environ['AzureStorageAccount']
    # sb_connection_str = os.environ['AzureServiceBus']

    # Retrieve a reference to the table.
    table_name = "OpsToNav2UpdateDateTimeTest"
    table_service = TableService(connection_string=azure_storage_account_str)
    table_service.create_table(table_name,fail_on_exist=False)

    # # Define the entity properties
    pk = "Last Updated Date"
    rk = "db-opsmgmt.workforce.personnel"
    # last_personnel_timestamp, last_details_timestamp, last_update_timestamp = "", "", ""
    last_personnel_timestamp, last_details_timestamp = "", ""
    value, details_value = "",""
    records = Entity()
    records.PartitionKey = pk
    records.RowKey = rk
    records.last_personnel_timestamp = last_personnel_timestamp
    records.last_details_timestamp = last_details_timestamp
    # records.last_update_timestamp = last_update_timestamp

    ## Retrieve last lsn from azure table
    # table_service = TableService(connection_string='<storage_account_connection_string>')
    retrieved_result = None
    
    try:
        retrieved_result = table_service.get_entity(table_name, pk, rk)
        # last_personnel_timestamp = retrieved_result.last_updated_timestamp
        # last_details_timestamp = retrieved_result.last_details_timestamp
        logging.info("Successfully connected to Azure storage.")
    except Exception as e:
        logging.info("Error while connecting to Azure storage: "+ str(e))
        # return

    logging.info("Fetching Ops personnel changes...")
    ops_conn = pyodbc.connect(ops_conn_str)

    try:
        # Get most recent timestamp from the personnel database.
        # We're returning it as a bigint,
        # since there appears to be a bug when pulling the data
        # and then converting the hex string as a string.
        latest_personnel_update_query = (
            "SELECT TOP(1) CAST(p.timestamp AS BIGINT) "  # this guy
            "FROM workforce.personnelDetails d " 
            "JOIN workforce.personnel p on p.personnelID = d.personnelId " 
            "JOIN admin.branch b on b.branchCode = p.branchCode " 
            "WHERE d.driverTypeMobile NOT IN (0, 4) " 
                # driver-based branches only
                "AND b.driverBased = 1 " 
            "ORDER BY p.timestamp DESC;"
        )
        last_personnel_timestamp_cmd = ops_conn.cursor()
        last_personnel_timestamp_cmd.execute(latest_personnel_update_query)
        # We're forcing the timestamp into a 16-digit hex string here,
        # since fetching the data raw saves it as a System.Byte[].
        # Also to ensure data consistency.
        # logging.info("0x{:016X}".format(int(last_personnel_timestamp_cmd.execute_scalar())))
        last_personnel_timestamp = "0x{:016X}".format(last_personnel_timestamp_cmd.fetchone()[0])
        # last_personnel_timestamp = "0x{:016X}".format(int(last_personnel_timestamp_cmd.execute_scalar()))

    except:
        logging.error("Failed in query/conversion from personel opsmgmt timestamp close db connection")
        ops_conn.close()
        return
    
    # logging.info(f"Last personnel timestamp in normal format: {datetime.datetime.fromtimestamp(last_personnel_timestamp_cmd.fetchone()[0]).strftime('%Y-%m-%d %H:%M:%S')}")
    # logging.info("0x{:016X}".format(last_personnel_timestamp_cmd.fetchone()[0]))

    try:
        # details
        latest_details_update_query = (
            "SELECT TOP(1) CAST(d.timestamp AS BIGINT) "  # this guy
            "FROM workforce.personnelDetails d " 
            "JOIN workforce.personnel p on p.personnelID = d.personnelId " 
            "JOIN admin.branch b on b.branchCode = p.branchCode " 
            "WHERE d.driverTypeMobile NOT IN (0, 4) " 
                # driver-based branches only
                "AND b.driverBased = 1 " 
            "ORDER BY d.timestamp DESC;"
        )
        last_details_timestamp_cmd = ops_conn.cursor()
        last_details_timestamp_cmd.execute(latest_details_update_query)
    
        # logging.info(str(last_personnel_timestamp_cmd.fetchone()[0]))
        # logging.info(f"Last details timestamp in normal format: {datetime.datetime.fromtimestamp(last_details_timestamp_cmd.fetchone()[0]).strftime('%Y-%m-%d %H:%M:%S')}")
        # we're forcing the timestamp into a 16-digit hex string here,
        # since fetching the data raw saves it as a System.Byte[].
        # Also to ensure data consistency.
        # last_details_timestamp = "0x" + format(last_details_timestamp_cmd.fetchone()[0], "016X")
        last_details_timestamp = "0x{:016X}".format(last_details_timestamp_cmd.fetchone()[0])

    except:
        logging.error("Failed in query/conversion of details timestamp from personnel opsmgmt, close db connection")
        ops_conn.close()
        return


    # Exit if no timestamp field found in table.
    if not last_personnel_timestamp or not last_details_timestamp:
        logging.info("No updated timestamps found. Exiting.")
        ops_conn.close()
        return

    # If last lsn value is available
    if retrieved_result:
        logging.info("Fetching most recently run timestamp...")
        logging.info(f"Last personnel timestamp: {retrieved_result.last_personnel_timestamp}")
        logging.info(f"Last details timestamp: {retrieved_result.last_details_timestamp}")
        # table_service.insert_or_replace_entity(table_name, records)
    else:
        logging.info("Most recent timestamp not available.")
        records.last_personnel_timestamp = last_personnel_timestamp
        records.last_details_timestamp = last_details_timestamp
        
        # Add entity to the azure storage table.
        table_service.insert_or_replace_entity(table_name, records)
        logging.info(f"Last timestamp value inserted in Azure Table Storage: {last_personnel_timestamp}")
        logging.info(f"Last details timestamp value inserted in Azure Table Storage: {last_details_timestamp}")
        value = last_personnel_timestamp
        details_value = last_details_timestamp

################################################################### working 22ndFeb up to here

    if value != last_personnel_timestamp or details_value != last_details_timestamp:
        personnel_last_run = retrieved_result.last_personnel_timestamp
        details_last_run = retrieved_result.last_details_timestamp
        # ops_conn = pyodbc.connect(ops_conn_str)

        ops_driver_changes_query = (
            "SELECT "
            "p.personnelID AS ops_driver_id, "
            "p.nameLast AS lastName, "
            "COALESCE(NULLIF(p.namePreferred, ''), p.nameFirst) AS firstName, "
            "b.branchCode AS division_id, "
            "d.driverHomeTerminalMobile AS homedivn, "
            "d.driverPinMobile AS pin, "
            "d.driverTypeMobile AS dtype, "
            "d.driverLicenseNo AS driverLicenseNo, "
            "p.countryCode, "
            "COALESCE(d.driverLicenseProv, '') AS licenseIssuingState, "
            "c.companyCode AS company, "
            "b.branchName as division_name, "
            "p.inactiveFlag as inactiveFlag, "
            "COALESCE(d.driverEmailMobile, '') AS driverEmailMobile "
            "FROM workforce.personnelDetails d "
            "JOIN workforce.personnel p ON p.personnelID = d.personnelId "
            "JOIN admin.branch b ON b.branchCode = p.branchCode "
            "JOIN admin.company c on b.companyCode = c.companyCode "
            "WHERE d.driverTypeMobile NOT IN (0, 4) " +
            # f"AND (p.timestamp > {personnel_last_run} OR d.timestamp > {details_last_run});"
            f"AND (p.timestamp > {personnel_last_run} OR d.timestamp > {details_last_run});"
        )

        ops_driver_change_transactions = []
        try:
            # rows = []
            driver_changes = ops_conn.cursor()
            driver_changes.execute(ops_driver_changes_query)
            # driver_changes = await ops_conn.QueryAsync<OpsDriverTransactions>(ops_driver_changes_query)
            # ops_driver_change_transactions = driver_changes if isinstance(driver_changes, list) else driver_changes.tolist()
            # rows.append(driver_changes.fetchall())
            # logging.info(rows)
            # using list comprehension to append instances to list
            # rows = driver_changes.fetchall()

            # row = driver_changes.fetchone()
            # while row:
            #     ops_driver_transactions = OpsDriverTransactions(row.ops_driver_id
            #                                     ,row.homedivn
            #                                     ,row.firstname
            #                                     ,row.lastname
            #                                     ,row.division_id
            #                                     ,row.division_name
            #                                     ,row.inactiveFlag
            #                                     ,row.timestamp
            #                                     ,row.driverEmailMobile
            #                                     ,row.dtype
            #                                     ,row.pin
            #                                     ,row.driverLicenseNo
            #                                     ,row.countryCode
            #                                     ,row.licenseIssuingState)
            #     ops_driver_change_transactions.append(ops_driver_transactions)
            #     row = driver_changes.fetchone()
            
            for driver in driver_changes.fetchall():
                # ops_driver_transactions = OpsDriverTransactions(driver[0],driver[1],driver[2],driver[3],driver[4],driver[5],driver[6],driver[7],driver[8],driver[9],driver[10],driver[11],driver[12],driver[13])
                ops_driver_transactions = OpsDriverTransactions(driver.ops_driver_id
                                                                ,driver.lastName
                                                                ,driver.firstName
                                                                ,driver.division_id
                                                                ,driver.homedivn
                                                                ,driver.pin
                                                                ,driver.dtype
                                                                ,driver.driverLicenseNo
                                                                ,driver.countryCode
                                                                ,driver.licenseIssuingState
                                                                # ,driver.company
                                                                ,driver.division_name
                                                                ,driver.inactiveFlag
                                                                ,driver.driverEmailMobile                                                           
                                                                )
                ops_driver_change_transactions.append(ops_driver_transactions)

            # if rows != None:
                # ops_driver_change_transactions += [OpsDriverTransactions(ops_driver_id, homedivn, firstname, lastname, division_id, division_name, inactiveFlag, timestamp, driverEmailMobile, dtype, pin, driverLicenseNo, countryCode, licenseIssuingState) for ops_driver_id, homedivn, firstname, lastname, division_id, division_name, inactiveFlag, timestamp, driverEmailMobile, dtype, pin, driverLicenseNo, countryCode, licenseIssuingState in rows]
            # ops_driver_change_transactions += [OpsDriverTransactions(ops_driver_id, homedivn, firstname, lastname, division_id, division_name, inactiveFlag, timestamp, driverEmailMobile, dtype, pin, driverLicenseNo, countryCode, licenseIssuingState) for ops_driver_id, homedivn, firstname, lastname, division_id, division_name, inactiveFlag, timestamp, driverEmailMobile, dtype, pin, driverLicenseNo, countryCode, licenseIssuingState in driver_changes.fetchall() if isinstance(driver_changes.fetchall(), list) else driver_changes.fetchall().to_list()]]
            # ops_driver_change_transactions = driver_changes.fetchall() if isinstance(driver_changes.fetchall(), list) else driver_changes.fetchall().to_list()


        except Exception as e:
            logging.error("Query could not be executed:"+ str(e))

            if "An insufficient number of arguments were supplied for the procedure" in str(e):
                records.last_personnel_timestamp = last_personnel_timestamp
                records.last_details_timestamp = last_details_timestamp

                table_service.insert_or_replace_entity(table_name, records)

                # insertOperation = TableOperation.insert_or_replace(records)

                # table.execute(insertOperation)

                logging.info(f"Last updated datetime value inserted in Azure Table Storage: {last_personnel_timestamp}")

            ops_conn.close()
            # ops_conn.dispose()

            raise

        logging.warning(f"Personnel changes found: {len(ops_driver_change_transactions)}")
        logging.info(ops_driver_change_transactions)

        records.last_personnel_timestamp = last_personnel_timestamp
        records.last_details_timestamp = last_details_timestamp

        table_service.insert_or_replace_entity(table_name, records)
        # insertOperation = TableOperation.insert_or_replace(records)


        # table.execute(insertOperation)

        # If driver transactions data structure not empty then send contents to the service bus




    ops_conn.close()


    # #############################################################

    if ops_driver_change_transactions:
        try:
            # await send_message_to_topic()
            logging.info("Attempting to send message to service bus...")
            # loop = asyncio.new_event_loop()
            # asyncio.set_event_loop(loop)
            # loop.run_until_complete(send_message_to_topic(ops_driver_change_transactions))
            # loop.close()
            # asyncio.run(send_message_to_topic(ops_driver_change_transactions))
            await send_message_to_topic(ops_driver_change_transactions)
            logging.info("Message successfully sent to service bus.")
        except Exception as e:
            logging.error("Sending message to service bus failed:"+ str(e))
            # loop.close()

if __name__ == "__main__":
    asyncio.run(main(func.TimerRequest("Timer", None)))

# if __name__ == "__main__":
#     # Call the async function using asyncio.run()
#     asyncio.run(main(None))


    #     try:
    #         # Send messages to service bus
    #         logger = logging.getLogger('azure')
    #         sb_client = ServiceBusClient.from_connection_string(sb_connection_str, logger=logger)
    #         # topic_sender = topic_client.get_topic_sender(sb_topic_name)
    #         topic_client = sb_client.get_topic(sb_topic_name)

    #         message = ServiceBusMessage()
    #         message.user_properties = {'Data': json.dumps(ops_driver_change_transactions)}

    #         logging.warning("Attempting to send data to the Service Bus: ")
    #         await topic_client.send_messages(message)

    #         logging.info("Message sent successfully.")
    #         logging.info(f"Message sent to topic driverintegration:\n {json.dumps(ops_driver_change_transactions)}")

    #         await topic_client.close()

    #         logging.info(f"Last updated date value inserted in Azure Table Storage: {last_personnel_timestamp}")#should actually use timestamp in azure storage table

    #     except Exception as e:
    #         logging.error("Could not send message to Service Bus:", exc_info=True)
    #         raise

    # else:
    #     logging.info("No changes available in the workforce.personnelDetails table.")


    # # logging.info(f"Last run timestamp: {last_personnel_timestamp}")

    # # If last lsn value is available
    # if retrieved_result:
    #     logging.info("Fetching most recently run timestamp...")
    #     logging.info(f"Last personnel timestamp: {retrieved_result.last_updated_timestamp}")
    #     logging.info(f"Last details timestamp: {retrieved_result.last_details_timestamp}")
    # else:
    #     logging.info("Most recent timestamp not available.")
    #     records = Entity()
    #     records.PartitionKey = pk
    #     records.RowKey = rk
    #     records.lastUpdated



##########################################################
    # entity = Entity()
    # entity.PartitionKey = pk
    # entity.RowKey = rk

    # records = LastDriverLUpdatedTime(pk, rk,last_details_timestamp, last_updated_timestamp)

    # Insert the entity
    # table_service.insert_or_replace_entity(table_name, entity)

    # Retrieve last lsn from Azure table
    # retrieved_entity = table_service.get_entity(table_name, pk, rk)
   
    # This is trying to get the 2 timestamps from the azure storage table. Not created yet.
    # last_personnel_timestamp, last_details_timestamp = retrieved_entity.get('last_updated_timestamp'), retrieved_entity.get('last_details_timestamp')

    # # try:
    # table_service = TableService(connection_string=azure_storage_account)
    # table_name = "OpsToNav2UpdateDateTime"
    # table_service.create_table(table_name, fail_on_exist=False)

    # # Create entity.
    # pk = "Last Updated Date"
    # rk = "db-opsmgmt.workforce.personnel"
    # entity = Entity(partition_key=pk, row_key=rk)


    # # Check if the table exists
    # if not table_service.exists(table_name):
    #     # Create the table if it doesn't exist
    #     table_service.create_table(table_name)

    #     print(f"Table {table_name} created successfully")
    # else:
    #     print(f"Table {table_name} already exists")

    # conn = pyodbc.connect(conn_str)

 

#     # Get most recent timestamp from the personnel database.
#     # We're returning it as a bigint,
#     # since there appears to be a bug when pulling the data
#     # and then converting the hex string as a string.
#     most_recent_personnel_update_query = """
#     SELECT TOP(1) CAST(p.timestamp AS BIGINT)
#     FROM workforce.personnelDetails d
#     JOIN workforce.personnel p on p.personnelID = d.personnelId
#     JOIN admin.branch b on b.branchCode = p.branchCode
#     WHERE d.driverTypeMobile NOT IN (0, 4)
#     AND b.driverBased = 1
#     ORDER BY p.timestamp DESC;
#     """
#     cursor = conn.cursor()
#     cursor.execute(most_recent_personnel_update_query)
#     last_personnel_timestamp = '0x' + format(int(cursor.fetchone()[0]), 'X').zfill(16)

#     most_recent_details_update_query = """
#     SELECT TOP(1) CAST(d.timestamp AS BIGINT)
#     FROM workforce.personnelDetails d
#     JOIN workforce.personnel p on p.personnelID = d.personnelId
#     JOIN admin.branch b on b.branchCode = p.branchCode
#     WHERE d.driverTypeMobile NOT IN (0, 4)
#     AND b.driverBased = 1
#     ORDER BY d.timestamp DESC;
#     """
#     cursor.execute(most_recent_details_update_query)
#     last_details_timestamp = '0x' + format(int(cursor.fetchone()[0]), 'X').zfill(16)

#     if last_personnel_timestamp is None or last_details_timestamp is None:
#         logging.warning('No updated timestamps found. Exiting.')
#         return

#     logging.info(f'Last run timestamp: {last_personnel_timestamp}')

#     # Save max lsn
#     if retrieved_entity is not None:
#         logging.info(f'Last personnel timestamp: {retrieved_entity.last_updated_timestamp}')
#         logging.info(f'Last details timestamp: {retrieved_entity.last_details_timestamp}')
#     else:
#         logging.info('Most recent timestamp not available.')
#         # Add entity to the Azure table.
#         entity.last_updated_timestamp = last_personnel_timestamp
#         entity.last_details_timestamp = last_details_timestamp
#         # table_service.insert_or_replace_entity(table




# # except Exception as e:
# #     log.error(f"OpsMgmt Production Server: {e}")
# #     raise

