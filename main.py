# simple script to read JPay settlement file, and then write the required Zenith file to [inputfile].zenith.txt
#
# pelle.vehreschild@jumia.com
#
# file format as requested by Zenith bank attached to ticket https://jira.jumia.com/browse/AFRPAY-11793
# Zenith_Bank_PFs_Merchant_Settlement_File_Specification.pdf
#
# as a quick summary, JumiaPay generates a CSV file while Zenith expects a file with fixed field lengths, a header record and a footer record with checksums
# 
# there are two hacks in this script:
# - we are in theory required to differentiate if the settlement is for POS, MPGS transactions or some other types - this will always be set to MPGS
# - Zenith allows only 50 characters for the settlement id, whereas JPay's ids are longer; this script will send the RIGHTMOST 50 charcters
# -- reason: we need a recognizable ID for reconciliation and can't generated a new ID
#
# script exits with exitcode 0 on success, or !=0 in case of an error
# output to stdout either starts with [OK] or [NOK] for Not OK in case of a fatal error

import csv, uuid, sys, re, os
from operator import truediv
import locale
import logging
import pysftp
import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from argparse import ArgumentParser
from dotenv import load_dotenv
import util,time
from threading import Timer
import jnius_config
#jnius_config.add_options('-Xrs', '-Xmx4096')
jnius_config.set_classpath('./java/masked-id-util.jar','./java/trimplement-wallet-server-common.jar')
import jnius
from jnius import autoclass
from datetime import datetime


print(os.getenv("ZENITH_ENV"))
load_dotenv(os.getenv("ZENITH_ENV"))

# fields we need in the JPay file to process it

jpayDict=["Merchant ID", "Shop Name", "Bank code", "Bank account number", "Settlement ID", "Settlement amount", "Fees+VAT", "Currency"]



def main() -> int:
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
        handlers=[
            logging.FileHandler("JPay2Zenith.log"),
            logging.StreamHandler(sys.stdout)
        ]
        , encoding='utf-8', level=logging.INFO)

    logging.info("Current locale: "+str(locale.getlocale(category=locale.LC_NUMERIC)))

    try:
        locale.setlocale(locale.LC_NUMERIC, "en_US")
    except Exception as e:
        exc_tb = sys.exc_info()
        logging.critical("Could not configure Locale en_us"+exc_tb)
        return 1
        
    logging.info("New locale: "+str(locale.getlocale(category=locale.LC_NUMERIC)))
    #start()
    while(True):
        start()
        time.sleep(int(os.getenv("JOB_INTERVAL"))*60)
    



def start():
    try:
        s3 = boto3.resource(
            's3'
        )
        print(os.getenv('S3_BUCKET'))
        bucket=s3.Bucket(os.getenv('S3_BUCKET'));
        for obj in bucket.objects.all():
            try:
                if not obj.key.startswith("NG/2022/"+datetime.now().strftime('%m')):
                    continue
                if not obj.key.endswith(".csv"):
                    continue
                if not obj.key.endswith("settlement-2022-09-20.csv"):
                    continue
                logging.info("Processing file "+obj.key)
                    
                fileName=obj.key[obj.key.rfind("/")+1:]
                path=obj.key[:obj.key.rfind("/")+1]
                outputFileName=fileName+".zenith.txt"
                print(fileName)
                print(path)
                path="NG/"
                print(outputFileName)
                try:
                    s3.Object(bucket.name, path+'Zenith/'+outputFileName).load()
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logging.info("File "+obj.key+ " doesn't exists")
                        bucket.download_file( obj.key,fileName )
                        NGNPay,NGNCom,USDPay,USDCom,cnt=writeSettlementFile(fileName,outputFileName,os.getenv('AGG_ID'))
                        
                        os.remove(fileName)
                        
                        if cnt==0:
                            sendWarningEmail(obj.key,outputFileName,NGNPay,NGNCom,USDPay,USDCom)
                        else:
                            transferSettlementFile(outputFileName);
                            bucket.upload_file(outputFileName,path+'Zenith/'+outputFileName)
                            os.remove(outputFileName)
                            sendSuccessEmail(obj.key,outputFileName,NGNPay,NGNCom,USDPay,USDCom)
                    else:
                        logging.error("Error checking if file  exists")
                        raise
                else:
                    logging.info("File "+ outputFileName+ " already integrated ")# Th
            except Exception as e:
                sendErrorMail(obj.key,e) 
            #return
    except Exception as e:
        sendErrorMail(None,e) 
    print("End")


def sendEmail(RECIPIENT,SUBJECT,BODY_TEXT,BODY_HTML):
    SENDER = "Nuno Ribeiro <nuno.ribeiro@jumia.com>"
    CHARSET = "UTF-8"
    client = boto3.client('ses',region_name=os.getenv("EMAIL_AWS_DEFAULT_REGION"),
aws_access_key_id=os.getenv("EMAIL_AWS_ACCESS_KEY_ID"),
aws_secret_access_key=os.getenv("EMAIL_AWS_SECRET_ACCESS_KEY"))
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
    else:
        logging.info("Email sent! Message ID:"+response['MessageId'])

def sendErrorMail(file,exception):
    logging.info("Sending error mail "+str(exception))

    if (file is None):
        res=""
    else:
        res=file

    RECIPIENT = os.getenv("EMAIL_ERROR_RECIPIENT")
    SUBJECT = "Zenith Integration - Error"
    BODY_TEXT = ("File "+res+ " was not converted"+"\n"+
    "Error - "+str(exception))
    BODY_HTML = """<html>
        <head></head>
        <body>
        <h1>Zenith Integration</h1>
        <p>File """+file+ """ was not converted</p>
        <li>Error ="""+str(exception)+"""</li>
        </body>
        </html>
            """            
   
    sendEmail(RECIPIENT,SUBJECT,BODY_TEXT,BODY_HTML)
    

     #s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
def sendSuccessEmail(file,convertedFile,NGNPay,NGNCom,USDPay,USDCom) :
    logging.info("Sending sucess mail ")
    RECIPIENT = os.getenv("EMAIL_RECIPIENT")
    SUBJECT = "Zenith Integration"
    BODY_TEXT = ("File "+file+ " was converted and integrated on ZenithBank with the name "+convertedFile+"\n"+
    " - Settlement amount="+str(NGNPay)+" NGN \n"+
    " - Fees & Tax amount="+str(NGNCom)+" NGN \n"+
    " - Settlement amount="+str(USDPay)+" USD \n"+
    " - Fees & Tax  amount="+str(USDCom)+" USD \n")
    BODY_HTML = """<html>
        <head></head>
        <body>
        <h1>Zenith Integration</h1>
        <p>File """+file+ """ was converted and integrated on ZenithBank with the name """+convertedFile+"""</p>
        <li>Settlement amount="""+str(NGNPay)+""" NGN </li>
        <li>Fees & Tax amount="""+str(NGNCom)+""" NGN </li>
        <li>Settlement amount="""+str(USDPay)+""" USD </li>
        <li>Fees & Tax  amount="""+str(USDCom)+""" USD </li>
        </body>
        </html>
            """            
    CHARSET = "UTF-8"
    sendEmail(RECIPIENT,SUBJECT,BODY_TEXT,BODY_HTML)


def sendWarningEmail(file,convertedFile,NGNPay,NGNCom,USDPay,USDCom) :
    logging.info("Sending warning mail ")
    RECIPIENT = os.getenv("EMAIL_RECIPIENT")
    SUBJECT = "Zenith Integration - Warning"
    BODY_TEXT = ("File "+file+ " was converted with 0 rows")
    BODY_HTML = """<html>
        <head></head>
        <body>
        <h1>Zenith Integration</h1>
        <p>File """+file+ """ was converted with 0 rows</p>
        </body>
        </html>
            """            
    CHARSET = "UTF-8"
    sendEmail(RECIPIENT,SUBJECT,BODY_TEXT,BODY_HTML)

def transferSettlementFile(outputFileName):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None   
    
    try:
        logging.info(os.getenv("SFTP_SERVER"))
        with pysftp.Connection(os.getenv("SFTP_SERVER"), username=os.getenv("SFTP_USER") , password=os.getenv("SFTP_PASS") , cnopts=cnopts ) as sftp:
            with sftp.cd('/upload'): 
                sftp.put('./'+outputFileName)  	
        logging.info("File "+outputFileName+ " transfered")
        
        return 0
    except Exception as e:
        handleException("Error transfering file "+outputFileName+" "+str(e)+str(exc_tb), e)
      


def handleException(message, exception):


    exc_tb = sys.exc_info()
    if exception is None:
        logging.error(message)
        raise Exception(message)
    else:
        logging.error(message+". Internal Error -  {}".format(str(exception)))
        raise Exception(message+" {}".format(str(exception)))
# Writes settlement file in accordance with "Zenith Bank’s PFs Merchants’ Settlement File" specifications 2.0, dated 25th January 2021
# Format defintion is copied here from specification at the relevant parts (header, detail, trailer records) 


# Header Record
# Positions Attrib. Field Name Values
# 1 1–4 ns-4 Record Type “0000” – Header record type
# 2 5–36 an-32 File Name “PFs Settlement File”. Left-justified, trailing spaces.
# 3 37–57 an-20 Aggregagor ID Left-justified, trailing spaces
# 4 58–60 an-10 Settlement Date DD/MM/YYYY format. True settlement date.
# 5 61–111 an-50 Unique File Reference Left-justified, trailing spaces

# Detail Record
# Positions Attrib. Field Name Values Status
# 1 1–4 ns-4 Record Type “1111” – detail record type for PFs merchants settlement Mandatory
# 2 5-25 an-20 Aggregagor ID Type of message being interchanged. Left-justified, trailing spaces. Mandatory
# 3 26-46 an-20 SUB_MERCHANT_ID The sub merchannt ID. Left-justified, trailing spaces Mandatory
# 4 47 -97 an-50 SUB_MERCHANT_NAME The sub merchant name. Left-justified, trailing spaces. Mandatory
# 5 98 -101 an-3 SUB_MERCHANT_BANK_CODE The sub merchant bank code. Mandatory
# 6 102 -112 ns-10 SUB_MERCHANT_ACCOUNT The sub merchant bank account number. Mandatory
# 7 113 -128 an-15 TRANSACTION_ TYPE The transaction type. Left-justified, trailing spaces. Mandatory
# 8 129 -179 an-50 UNIQUE TRANSACTION REF A global unique identifier for this transaction. Left-justified, trailing spaces. Mandatory
# 9 180 -230 an-50 TRANSACTION ID The transaction ID. Left-justified, trailing spaces. Mandatory
# 10 231 -243 n-12 TRANSACTION_AMOUNT Settlement Amount to be creditted to the merchant. Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point. Mandatory
# 11 244-256 n-12 COMMISSION_AMOUNT PFs commission to be creditted to the PF. Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point. Mandatory
# 12 257-260 an-3 CURRENCY(NGN/USD) The currency of settlement transaction. Format (NGN/USD) Mandatory
# 13 261-271 an-10 SETTLEMENT_DATE The settlment date. Date format (dd/mm/yyyy) Mandatory
# 14 272 -287 an-15 SWIFT_BIC The swift BIC of the offshore bank. Left-justified, trailing spaces. Optional
# 15 288 -308 an-20 OFFSHORE_ACCOUNT The merchants' offshore bank account number. Left-justified, trailing spaces. Optional
# 16 309 -329 an-20 CHANNEL_TYPE The channel code of the transaction. See legend below for applicable code. Left-justified, trailing spaces. Mandatory

# Positions Attrib. Field Name Values
# 1 1–4 ns-4 Record Type “2222” – Trailer record type
# 2 5–13 n-8 Number of financial transactions Right-justified, leading zeros
# 3 14-26 n-12 Sum of Transaction Amount (NGN) Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point. 
# 4 27–39 n-12 Sum of Commission Amount (NGN) Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point. 
# 5 40–52 n-12 Sum of Transaction Amount (USD) Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point. 
# 6 53–65 n-12 Sum of Commission Amount (USD) Right-justified, leading zeros. All currency amounts are in the minor unit of currency without a decimal point.

def writeSettlementFile(inputFileName, outputFileName, aggregatorid):
    tmp=("0000JPay Settlement File                                                            ")[0:36]
    tmp=tmp+(util.alnum(aggregatorid)+"                                                            ")[0:20]
    tmp=tmp+(datetime.today().strftime('%d/%m/%Y'))
    tmp=tmp+(util.alnum(str(uuid.uuid1()))+"                                                            ")[0:50]

    transactionType="Settlement"

    output=[tmp]
    #print (tmp)

    cnt=0
    NGNPay=0
    NGNCom=0
    USDPay=0
    USDCom=0

# build an array in memory with the outpt file, then dump it out once input file has been parsed
# since this is grouped by merchant, risk of running out of memory is neglegtable
# benefit is that we either have a complete file or none, but do not run the risk of having to manually clean up a half-processed file
    try:
        MaskedUtil = autoclass('MaskedUtil')
        maskedUtil = MaskedUtil()
    
        with open(inputFileName, newline='') as csvfile:
            jpay = csv.DictReader(csvfile, delimiter=',', quotechar='\"')
            for row in jpay:
                cnt=cnt+1
            
                for dict in jpayDict:
                    if dict not in row:
                        handleException("Row "+str(cnt)+" does not have "+dict,None)
                settlementAmount=-1
                try:
                    if "-" in row["Settlement amount"]:
                        raise Exception("Settlement value is negative")
                    settlementAmount=Decimal(util.clearCurrencyFormat(row["Settlement amount"]))
                    logging.info("settlementAmount "+row["Settlement amount"]+" converted to "+str(settlementAmount))
                    
                except Exception as e:
                   handleException("Settlement amount \""+row["Settlement amount"]+"\" in row "+str(cnt)+" is not a decimal",e)
                vatAndFees=-1
                try:
                    vatAndFees=Decimal(util.clearCurrencyFormat(row["Fees+VAT"]))
                    logging.info("vatAndFees "+row["Fees+VAT"]+" converted to "+str(vatAndFees))
                except  Exception as e:
                    handleException("Fees+VAT \""+row["Fees+VAT"]+"\" in row "+str(cnt)+" is not a decimal",e)
                if os.getenv("ZENITH_MASK")=="v1":
                    settlementIdZenith=maskedUtil.getZenithPublicId(maskedUtil.getJumiaInternalId( row["Settlement ID" ]))
                else:
                    settlementIdZenith=maskedUtil.getZenithPublicIdV2(maskedUtil.getJumiaInternalId( row["Settlement ID" ]))

                tmp="1111" #record type
                tmp=tmp+(util.alnum(aggregatorid)+"                                                            ")[0:20]
                tmp=tmp+(util.alnum(row["Merchant ID"])+"                                                            ")[0:20]
                tmp=tmp+(util.alnum(row["Shop Name"])+"                                                            ")[0:50]
                tmp=tmp+(util.alnum(row["Bank code"])+"                                                            ")[0:3]
                tmp=tmp+(util.alnum(row["Bank account number"])+"                                                            ")[0:10]
                tmp=tmp+(util.alnum(transactionType)+"                                                            ")[0:15]
                tmp=tmp+(util.alnum(settlementIdZenith)+"                                                            ")[0:50]
                tmp=tmp+(util.alnum(settlementIdZenith)+"                                                            ")[0:50]
                tmp=tmp+("000000000000"+str(int(settlementAmount*100)))[-12:]
                tmp=tmp+("000000000000"+str(int(vatAndFees*100)))[-12:]
                tmp=tmp+(util.alnum(row["Currency"])+"                                                            ")[0:3]
                tmp=tmp+(datetime.today().strftime('%d/%m/%Y'))
                tmp=tmp+("                                                            ")[0:35]
                tmp=tmp+("MPGS                                                            ")[0:20]
                output.append(tmp)

                if row["Currency"]=="NGN":
                    NGNPay=NGNPay+int(settlementAmount*100)
                    NGNCom=NGNCom+int(vatAndFees*100)
                elif row["Currency"]=="USD":
                    USDPay=USDPay+int(settlementAmount*100)
                    USDCom=USDCom+int(vatAndFees*100)
                else:
                    handleException("Unknown currency "+row["Currency"],None)
        tmp="2222" #record type
        tmp=tmp+("000000000000"+str(int(cnt)))[-8:]
        tmp=tmp+("000000000000"+str(NGNPay))[-12:]
        tmp=tmp+("000000000000"+str(NGNCom))[-12:]
        tmp=tmp+("000000000000"+str(USDPay))[-12:]
        tmp=tmp+("000000000000"+str(USDCom))[-12:]

        output.append(tmp)

        logging.info('Finished reading file with '+str(cnt)+' lines')
        if cnt==0:
           return NGNPay/100,NGNCom/100,USDPay/100,USDCom/100,cnt 
    except Exception as e:
        exc_tb = sys.exc_info()
        handleException("Exception "+str(e.__class__)+" when processing file "+inputFileName+" in line "+str(cnt),e)
    cnt=0
    try:
        logging.info('Writing '+outputFileName)
        
        with open(outputFileName, 'w') as f:
            for item in output:
                cnt=cnt+1
                f.write("%s\n" % item)
    
        logging.info('Finished writing file with '+str(cnt)+' lines')
    except Exception as e:
        exc_tb = sys.exc_info()
        handleException("Exception "+str(e.__class__)+"  when processing file "+inputFileName+" in line "+str(cnt),e)
    return NGNPay/100,NGNCom/100,USDPay/100,USDCom/100,cnt
    
if __name__ == '__main__':
    sys.exit(main())


