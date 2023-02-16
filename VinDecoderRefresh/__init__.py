import datetime
import logging
import os

import azure.functions as func

from .vindecoder import get_vin_list, decode_vins, clean_decoded_vins, update_insert_table


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        logging.info('Retrieving VINs from DTMS using '+os.environ["VINS_SOURCE_SERVER"]+' and '+os.environ["VINS_SOURCE_DATABASE"])
        logging.info("\n")
        vin_list = get_vin_list(os.environ["VINS_SOURCE_SERVER"], os.environ["VINS_SOURCE_DATABASE"])
        logging.info("Number of new VINs: "+str(len(vin_list)))
        logging.info("List of VINs to add: "+vin_list.to_string(index=False))

    except Exception as Argument:
        logging.exception('Error occurred while retrieving VINs from DTMS using '+os.environ["VINS_SOURCE_SERVER"]+' and '+os.environ["VINS_SOURCE_DATABASE"])
        logging.info(Argument)
    
    if len(vin_list)!=0:
        try:
            logging.info('Passing VINs to Vin Decoding API...')
            decoded_vins = decode_vins(vin_list)
            logging.info('VINS successfully decoded')
            
        except Exception as Argument:
            logging.exception('Error while attempting to decode the VINS')
            logging.info(Argument)

        try:
            logging.info('Cleaning decoded VINS API output....')
            decoded_vins_clean = clean_decoded_vins(decoded_vins)
        
        except Exception as Argument:
            logging.exception('Error while attempting to clean the decoded VINS')
            logging.info(Argument)

        try:
            # Update the table storing decoded VIN info
            update_insert_table(
                decoded_vins_clean,
                os.environ['VINS_TARGET_SCHEMA']+"."+os.environ['VINS_TARGET_TABLE'],
                os.environ['VINS_TARGET_SERVER'],
                os.environ['VINS_TARGET_DATABASE'],
            )
        except Exception as Argument:
            logging.exception('Error while attempting to update/insert VINs to DW.')
            logging.info(Argument)
    else:
        logging.info("No new VINs have been added to DTMS since the last refresh.")