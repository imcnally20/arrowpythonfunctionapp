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

        logging.info("Completed sending message to service bus.")

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
            "division_name": self.division_name,
            "inactiveFlag": self.inactiveFlag,
            "driverEmailMobile": self.driverEmailMobile,
        }

async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    ops_conn_str = os.environ['opsConnection']
    azure_storage_account_str = os.environ['AzureStorageAccount']

    # Retrieve a reference to the table.
    table_name = "OpsToNav2UpdateDateTimeTest"
    table_service = TableService(connection_string=azure_storage_account_str)
    table_service.create_table(table_name,fail_on_exist=False)

    # # Define the entity properties
    pk = "Last Updated Date"
    rk = "db-opsmgmt.workforce.personnel"
    last_personnel_timestamp, last_details_timestamp = "", ""
    value, details_value = "",""
    records = Entity()
    records.PartitionKey = pk
    records.RowKey = rk
    records.last_personnel_timestamp = last_personnel_timestamp
    records.last_details_timestamp = last_details_timestamp

    ## Retrieve last lsn from azure table
    # table_service = TableService(connection_string='<storage_account_connection_string>')
    retrieved_result = None
    
    try:
        retrieved_result = table_service.get_entity(table_name, pk, rk)
        logging.info("Successfully connected to Azure storage.")
    except Exception as e:
        logging.info("Error while connecting to Azure storage: "+ str(e))

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
        last_personnel_timestamp = "0x{:016X}".format(last_personnel_timestamp_cmd.fetchone()[0])

    except:
        logging.error("Failed in query/conversion from personel opsmgmt timestamp close db connection")
        ops_conn.close()
        return

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

    if value != last_personnel_timestamp or details_value != last_details_timestamp:
        personnel_last_run = retrieved_result.last_personnel_timestamp
        details_last_run = retrieved_result.last_details_timestamp

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
            f"AND (p.timestamp > {personnel_last_run} OR d.timestamp > {details_last_run});"
        )

        ops_driver_change_transactions = []
        try:
            driver_changes = ops_conn.cursor()
            driver_changes.execute(ops_driver_changes_query)
            
            for driver in driver_changes.fetchall():
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
                                                                ,driver.division_name
                                                                ,driver.inactiveFlag
                                                                ,driver.driverEmailMobile                                                           
                                                                )
                ops_driver_change_transactions.append(ops_driver_transactions)

        except Exception as e:
            logging.error("Query could not be executed:"+ str(e))

            if "An insufficient number of arguments were supplied for the procedure" in str(e):
                records.last_personnel_timestamp = last_personnel_timestamp
                records.last_details_timestamp = last_details_timestamp

                table_service.insert_or_replace_entity(table_name, records)

                logging.info(f"Last updated datetime value inserted in Azure Table Storage: {last_personnel_timestamp}")

            ops_conn.close()

            raise

        logging.warning(f"Personnel changes found: {len(ops_driver_change_transactions)}")
        logging.info(ops_driver_change_transactions)

        records.last_personnel_timestamp = last_personnel_timestamp
        records.last_details_timestamp = last_details_timestamp

        table_service.insert_or_replace_entity(table_name, records)

    ops_conn.close()

    if ops_driver_change_transactions:
        try:
            logging.info("Attempting to send message to service bus...")
            await send_message_to_topic(ops_driver_change_transactions)
            logging.info("Message successfully sent to service bus.")
        except Exception as e:
            logging.error("Sending message to service bus failed:"+ str(e))

if __name__ == "__main__":
    asyncio.run(main(func.TimerRequest("Timer", None)))