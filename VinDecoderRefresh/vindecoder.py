import io
import os 
import sys
import time
from datetime import datetime

import pandas as pd
import pyodbc
import requests
import logging


def get_vin_list(server, database):
    """
    Retrieves a list of VINs from a SQL Server database based on certain conditions.

    Parameters

    server : str
    The name or IP address of the SQL Server.
    database : str
    The name of the database.

    Returns

    vin_list : list
    A list of VINs satisfying the conditions specified in the query.
    """

    # Create the connection object
    cnxn = pyodbc.connect(
        "DRIVER="
        # +"{SQL Server Native Client 11.0}"
        +"ODBC Driver 17 for SQL Server"
        # +"ODBC Driver 18 for SQL Server"
        # +"{SQL Server}"
        +";SERVER="
        + server
        + ";DATABASE="
        + database
        + ";PORT=1433"
        +";AUTHENTICATION=SqlPassword"
        # +";THREADS=1"
        # + ";TrustedServerCertificate=YES"
        # + ";Trusted_Connection=YES"
        + ";ENCRYPT=NO"
        + ";UID="
        + os.environ['USER']
        + ";PWD="
        + os.environ['DWPWD']
    )
    # %%
    query = """
      select distinct
      pbu.code as code,
      pbu.serial as serial,
      pbu.homedivn as homedivn
      FROM {}.{}.{} pbu
      LEFT JOIN {}.{}.{} vd
      ON vd.vin = pbu.serial
      where pbu.vclass IN ('TRUCK','TRUCKSLP')
      and (pbu.inact_date IS NULL or pbu.inact_date >= DateAdd(yy, -1, GetDate()))
      and vd.vin is null
    """.format(os.environ["VINS_SOURCE_DATABASE"],
               os.environ["VINS_SOURCE_SCHEMA"],
               os.environ["VINS_SOURCE_TABLE"],
               os.environ["VINS_TARGET_DATABASE"],
               os.environ["VINS_TARGET_SCHEMA"],
               os.environ["VINS_TARGET_TABLE"])

    # %%
    # Perform query and read into dataframe
    df = pd.read_sql(query, cnxn)

    # %%
    cnxn.close()

    # %%
    vin_list = df[df["serial"].str.strip().str.len() == 17]["serial"]

    invalid_vin_list = df[df["serial"].str.strip().str.len() != 17]#["serial","code","homedivn"]

    if len(invalid_vin_list)!=0:
        for ind in invalid_vin_list.index:
            logging.info(invalid_vin_list["serial"][ind] +" VIN  is invalid. " \
                         +"Truck :"+invalid_vin_list["code"][ind] \
                         +" Home division: "+invalid_vin_list["homedivn"][ind] \
                         +". VIN Must be 17 characters." )

 
    return vin_list


def decode_vins(vin_list):
    """
    Decodes a list of vehicle identification numbers (VINs) using the National Highway Traffic Safety Administration (NHTSA) API.
    The function will return a dataframe containing the decoded VINs.

    Parameters:
    vin_list (List[str]): A list of strings representing the VINs that need to be decoded.

    Returns:
    pd.DataFrame: A dataframe containing the decoded VINs.
    """

    # Set up the dataframes
    decoded_vins = pd.DataFrame()
    df_tmp = pd.DataFrame()
    st_tmp = None

    lower = 0
    upper = 10

    while lower < len(vin_list):
        # Create temporary string for the request from the vin column of the temporary dataframe
        st_tmp = (
            "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
            + ";".join(vin_list[lower:upper])
            + ";?format=csv"
        )
        logging.info("Sending API request for items "+str(lower)+" to "+str(upper)+" in VIN list: "+st_tmp)
        
        # Perform the batch request for that set of 10 vins
        try:
            df_tmp = pd.read_csv(io.StringIO(requests.post(st_tmp).content.decode("utf-8")))
        except:
            logging.exception("Error while sending VINS to decoder API")

        # append to the final dataframe
        decoded_vins = pd.concat([decoded_vins, df_tmp])

        # update counters
        lower = lower + 10
        upper = upper + 10
        # delay before the next request
        time.sleep(1)
    return decoded_vins


# %%


def clean_decoded_vins(decoded_vins):
    """
    clean_decoded_vins(decoded_vins: pd.DataFrame) -> pd.DataFrame

    This function takes in a DataFrame of decoded VINs and performs several cleaning and transformation steps on it.

        It drops any column with all NaN values and keeps only the columns 'vin', 'make', 'model', 'modelyear',
        'drivetype', 'enginemanufacturer', 'enginemodel' and 'displacementl'
        It replaces NaN values in the 'modelyear' column with 0 and converts the column into an int data type
        It replaces values in the 'enginemodel' column with the correct value using a dictionary of corrections
        It strips whitespaces from the 'enginemanufacturer' column and replaces the value 'Cat' with 'Caterpillar'
        It replaces the 'enginemanufacturer' column values with 'Caterpillar' when the 'enginemodel' column is one
        of the following '3306/C15/C16', '3406/C15/C16', 'C12/C13', 'C-13 ACERT', 'C-15 ACERT', 'C15 (Displacement
        14.6 L pre 2008/15.2 L)' or '3126/C7'

    Returns:
    A cleaned and transformed DataFrame of decoded VINs.
    """

    decoded_vins_cleaned = decoded_vins.dropna(how="all", axis=1)[
        [
            "vin",
            "make",
            "model",
            "modelyear",
            "drivetype",
            "enginemanufacturer",
            "enginemodel",
            "displacementl",
        ]
    ]
    decoded_vins_cleaned.modelyear = decoded_vins_cleaned.modelyear.fillna(value=0)
    decoded_vins_cleaned.modelyear = decoded_vins_cleaned.modelyear.astype(int)

    # %%
    enginemodel_corrections = {
        "Diesel DD16": "Diesel DD16",
        "D16": "D16",
        "D13": "D13",
        "ISX 15L": "ISX",
        "ISX": "ISX",
        "X15, Signature ": "X15, Signature",
        "Diesel DD13": "Diesel DD13",
        "Diesel DD15": "Diesel DD15",
        "6B5.9": "6B5.9",
        "N-14NTC, F&PT": "N-14NTC, F&PT",
        "NT": "NT",
        "3406/C15/C16": "3406/C15/C16",
        "N14": "N14",
        "International Maxxforce 13": "International Maxxforce 13",
        "VE D16": "VE D16",
        "MP8": "MP8",
        "Detroit Diesel DD16": "Diesel DD16",
        "Detroit Diesel DD15": "Diesel DD15",
        "Detroit Diesel DD13": "Diesel DD13",
        "PACCAR MX/MX-13": "MX/MX-13",
        "Cummins ISX": "ISX",
        "Cummins Signature 600, ISX, AHD": "Signature 600, ISX, AHD",
        "Caterpillar C12/C13": "C12/C13",
        "Caterpillar 3306/C15/C16": "3306/C15/C16",
        "Caterpillar 3406/C15/C16": "3406/C15/C16",
        "Caterpillar 3406": "3406/C15/C16",
        "3406": "3406/C15/C16",
        "C15": "3406/C15/C16",
        "Caterpillar C-13 ACERT": "C-13 ACERT",
        "Caterpillar C-15 ACERT": "C-15 ACERT",
        "Cat C15 (Displacement 14.6 L pre 2008/15.2 L)": "C15 (Displacement 14.6 L pre 2008/15.2 L)",
        "Caterpillar 3126/C7": "3126/C7",
    }

    # %%
    decoded_vins_cleaned["enginemodel"] = decoded_vins_cleaned.enginemodel.map(
        enginemodel_corrections
    )
    decoded_vins_cleaned.enginemodel.map(enginemodel_corrections)

    # %%
    decoded_vins_cleaned.enginemanufacturer = (
        decoded_vins_cleaned.enginemanufacturer.str.strip()
    )
    decoded_vins_cleaned["enginemanufacturer"].loc[
        (decoded_vins_cleaned["enginemanufacturer"] == "Cat")
    ] = "Caterpillar"

    decoded_vins_cleaned["enginemanufacturer"].loc[
        (decoded_vins_cleaned["enginemodel"] == "3306/C15/C16")
        | (decoded_vins_cleaned["enginemodel"] == "3406/C15/C16")
        | (decoded_vins_cleaned["enginemodel"] == "C12/C13")
        | (decoded_vins_cleaned["enginemodel"] == "C-13 ACERT")
        | (decoded_vins_cleaned["enginemodel"] == "C-15 ACERT")
        | (
            decoded_vins_cleaned["enginemodel"]
            == "C15 (Displacement 14.6 L pre 2008/15.2 L)"
        )
        | (decoded_vins_cleaned["enginemodel"] == "3126/C7")
    ] = "Caterpillar"

    decoded_vins_cleaned["enginemanufacturer"].loc[
        (decoded_vins_cleaned["enginemodel"] == "Signature 600, ISX, AHD")
        | (decoded_vins_cleaned["enginemodel"] == "ISX")
        | (decoded_vins_cleaned["enginemodel"] == "X15, Signature")
    ] = "Cummins"

    decoded_vins_cleaned["enginemanufacturer"].loc[
        (decoded_vins_cleaned["enginemodel"] == "Diesel DD13")
        | (decoded_vins_cleaned["enginemodel"] == "Diesel DD15")
        | (decoded_vins_cleaned["enginemodel"] == "Diesel DD16")
    ] = "Detroit"

    # Replace null values with "NULL"
    decoded_vins_cleaned = decoded_vins_cleaned.fillna("NULL")

    # convert column to nvarchar(50)
    for col in decoded_vins_cleaned.columns:
        decoded_vins_cleaned[col] = decoded_vins_cleaned[col].astype(str)
        decoded_vins_cleaned[col] = decoded_vins_cleaned[col].str.slice(0, 50)
        decoded_vins_cleaned[col] = decoded_vins_cleaned[col].str.upper()

    return decoded_vins_cleaned


def update_insert_table(
    dataframe: pd.DataFrame, table_name: str, server: str, database: str
) -> None:
    """
    This function updates a SQL Server table with data from a given pandas DataFrame.

    Parameters:
    dataframe (pd.DataFrame): A pandas DataFrame containing the data to be updated in the table.
    table_name (str): The name of the table to be updated.
    server (str): The server name where the database is located.
    database (str): The name of the database to be updated.

    Returns:
    None

    Example:
    update_table(dataframe, "vehicle", "server_name", "database_name")
    """
    # Create the connection object
    conn = pyodbc.connect(
        "DRIVER={SQL Server Native Client 11.0};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";Trusted_Connection=Yes"
    )
    cursor = conn.cursor()

    # Iterate over the rows in the DataFrame
    for index,row in dataframe.iterrows():
        try:
            # Update the table with the data from the DataFrame
            cursor.execute(
                """
                IF EXISTS (SELECT * FROM {} WHERE vin = ?)
                    UPDATE {} SET make = ?, model = ?, modelyear = ?, drivetype = ?, 
                    enginemanufacturer = ?, enginemodel = ?, displacementl = ?
                    WHERE vin = ?
                ELSE
                    INSERT INTO {} (vin, make, model, modelyear, drivetype, enginemanufacturer, enginemodel, displacementl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".format(
                    table_name, table_name, table_name
                ),
                (
                    row["vin"],
                    row["make"],
                    row["model"],
                    row["modelyear"],
                    row["drivetype"],
                    row["enginemanufacturer"],
                    row["enginemodel"],
                    row["displacementl"],
                    row["vin"],
                    row["vin"],
                    row["make"],
                    row["model"],
                    row["modelyear"],
                    row["drivetype"],
                    row["enginemanufacturer"],
                    row["enginemodel"],
                    row["displacementl"],
                ),
            )
        except Exception as Argument:
            # Commit the changes and close the connection
            logging.exception('Error while attempting to update/insert VinsDecoded table.')
            logging.info(Argument)
            # conn.rollback()
            conn.close()

    # Commit the changes and close the connection
    conn.commit()
    conn.close()