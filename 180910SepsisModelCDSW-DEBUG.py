# Cloudera Data Science Workbench  notebook source / Ken Cottrell
import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import Row, Column
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc, round
from pyspark.conf import SparkConf
from pyspark.sql.functions import unix_timestamp, datediff

#import pyarrow.parquet as pq
#import numpy as np
#import pandas as pd
#from pandas import DataFrame, read_csv


#import StringIO

import pyspark.sql.functions as F
from pyspark.sql.functions import 


import pyspark.sql.types as T

import calendar;
import time;
myTimeStamp = str(calendar.timegm(time.gmtime()))



inputbucketName="kenshealthdata"
outBucketName = 'kenshealthdata/outputs' + myTimeStamp

chartEvents="CHARTEVENTS.csv"
patientDiagnoses = "DIAGNOSES_ICD.csv"
icdLabels = "D_ICD_DIAGNOSES.csv"
labLabels = "D_LABITEMS.csv"
labEvents =  "LABEVENTS.csv"
bioEvents = "MICROBIOLOGYEVENTS.csv"
patientsInfo = "PATIENTS.csv"
admissionsInfo = "ADMISSIONS.csv"
drgCodes = "DRGCODES.csv"


# vitals codes [sepsisRiskMarkersDict] dictionary , courtesy of this notebook:   
# https://github.com/j-crowe/ICU_Survival_ML_Model_Comparison
# Project Name: Survivability of ICU Patients with Severe Sepsis/Septic Shock
# Author: Jeremy B. Crowe
sepsisRiskMarkersDict = {   
                '50827': 'respiration', # respiration rate
                '50912': 'creatinine',  # creatinine levels indicate kidney issues
                '50813': 'lactate',     # lactate levels indicate shock and cellular anirobic respiration
                '50889': 'CRP',         # C-reactive protein systemic inflammation
                '51300': 'WBC',         # White blood cell count indicates systemic reaction to infection
                '51006': 'BUN',         # Blood Urea Nitrogen indicates kidney issues
                '51288': 'ESR',         # erythrocyte sedimentation rate another inflammation test
                '51265': 'platelet',    # decreased platelet counts parallel the severity of infection
                '50825': 'tempurature', # tempurature is highly indicative of infection and/or immune response
                '50816': 'oxygen',
                '51275': 'PT',
                '51274': 'PTT',
                '51277': 'RBCDW',       # red blood cell distribution width
                '51256': 'neutrophils',
                '50818': 'pco2',
                '50821': 'po2',
                '50893': 'calcium',
                '50931': 'glucose',
                '51221': 'hematocrit',
                '51222': 'hemoglobin',
                '51244': 'lymphocytes',
                
                '51248': 'MCH',
                '51237': 'INR',
                '50956': 'lipase',
                '50878': 'AST',
                '50867': 'amylase',
                '50863': 'alkaline phosphatase',
                '50820': 'PH',
                '50882': 'bicarbonate'
            } 


# need to add these
if 0:
    pd.set_option('display.mpl_style', 'default') 
    pd.set_option('display.line_width', 5000) 
    pd.set_option('display.max_columns', 60) 
    figsize(15, 5)


import matplotlib.pyplot as plt
import sys #only needed to determine Python version number
import matplotlib #only needed to determine Matplotlib version number

# Enable inline plotting
# %matplotlib inline / not supported in databricks use display() instead 
    
    
import os    # in case have to remove a file os.remove(<filenameS>)
 
from time import gmtime, strftime
showtime = strftime("%Y%m%d-%H%M%S", gmtime())
appName = "Chart Lab Diagnoses: " + showtime
 
if True:    # sc automatically set in databricks, set here for CDSW
  SparkConf().setMaster(value = "spark://ken-HP-EliteBook-8530w:7077")
  spark = SparkSession.builder.appName(appName).config(conf=SparkConf()).getOrCreate()
  
sc = spark.sparkContext
sqlctx = SQLContext(sc)

if False:    # snappy is the default compression
  sqlctx.setConf("spark.sql.parquet.compression.codec", "uncompressed") 



print(sc.master)
print(spark.sparkContext.getConf)
print('Python version ' + sys.version)
# print('Pandas version ' + pd.__version__)
print('Matplotlib version ' + matplotlib.__version__)


# COMMAND ----------

# set datadirectory to hdfs location: 
#     hdfs dfs -ls /user/cottrell_ken/data/*.csv
#datadirectory = "data/MIMICDEMODATA/"
# datadirectory = "/home/cdsw/resources/data/MIMICDEMODATA/"
# datadirectory = "data/MIMICDEMODATA/"

# from jacob link to full MIMIC data set
#http://35.237.75.192:50070/explorer.html#/ember/mimic/raw
datadirectory = "/ember/mimic/raw/"

chartEventsDataFile = datadirectory +  chartEvents
labEventsDataFile = datadirectory +  labEvents
diagnosesDataFile = datadirectory +  patientDiagnoses
diagnosesICDTableFile = datadirectory +  icdLabels
bioEventsDataFile = datadirectory + bioEvents
patientsDataFile = datadirectory + patientsInfo
admissionsDataFile = datadirectory + admissionsInfo
drgCodesDataFile = datadirectory + drgCodes


  # notesRawDF = pd.read_csv(noteEventsDataFile,names=notesColNames) # pandas DF
diagnoses1 = spark.read.csv(diagnosesDataFile)
labEvents1 = spark.read.csv(labEventsDataFile)
chartEvents1 = spark.read.csv(chartEventsDataFile)
diagICDLookup1 = spark.read.csv(diagnosesICDTableFile)
  # plaintextfile = sc.textFile('/home/ken/filename.txt').map(lambda line: line.split(","))
patients1 = spark.read.csv(patientsDataFile)
admissions1 = spark.read.csv(admissionsDataFile)
bioEvents1 = spark.read.csv(bioEventsDataFile)




# COMMAND ----------




# COMMAND ----------
"""
diagICDpeek = diagICDLookup1.select(col("_c1").alias("ICD9_CODE"),\
                                     col("_c2").alias("SHORT_TITLE"))
diagICDpeek.show(5)

+---------+--------------------+
|ICD9_CODE|         SHORT_TITLE|
+---------+--------------------+
|ICD9_CODE|         SHORT_TITLE|
|    01166|TB pneumonia-oth ...|
|    01170|TB pneumothorax-u...|
|    01171|TB pneumothorax-n...|
|    01172|TB pneumothorx-ex...|
+---------+--------------------+
"""


# COMMAND ----------
 
"""
subjectsTop20 = diagnoses1.groupBy("_c1").count()    # count Diagnoses ICD  per subject
subjectsTop20 = subjectsTop20.orderBy("count", ascending=False)  # show subjects with longest list of ICD
subjectsTop20 = subjectsTop20.select(col("_c1").alias("SUBJECT_ID"), col("count"))
subjectsTop20.show(5)
+----------+-----+
|SUBJECT_ID|count|
+----------+-----+
|     41976|  266|
|     10088|   57|
|     40310|   53|
|     42346|   41|
|     41795|   39|
+----------+-----+
"""

# COMMAND ----------
"""
diagTop20 = diagnoses1.groupBy("_c4").count()    # subject population count that express a given ICD
diagTop20 = diagTop20.orderBy("count", ascending=False)   # look for the 20 largest populations per ICD
diagTop20 = diagTop20.select(col("_c4").alias("ICD9CD"), col("count"))
diagTop20.show(5)
+------+-----+
|ICD9CD|count|
+------+-----+
|  4019|   53|
| 42731|   48|
|  5849|   45|
|  4280|   39|
| 51881|   31|
+------+-----+
"""


# COMMAND ----------
"""
diag6 = diagTop20.join(diagICDpeek, diagTop20.ICD9CD == diagICDpeek.ICD9_CODE, 'inner')
diag6.show(5)
+---------+-----+---------+--------------------+
|   ICD9CD|count|ICD9_CODE|         SHORT_TITLE|
+---------+-----+---------+--------------------+
|ICD9_CODE|    1|ICD9_CODE|         SHORT_TITLE|
|    00845|    7|    00845|Int inf clstrdium...|
|    04111|    1|    04111|Mth sus Stph aur ...|
|    04112|    2|    04112|  MRSA elsewhere/NOS|
|     0413|    1|     0413|Klebsiella pneumo...|
+---------+-----+---------+--------------------+
"""

# COMMAND ----------

subjectDiagnoses = diagnoses1.select(col("_c1").alias("SUBJECT_ID"), col("_c4").alias("ICD9_CODE")) #rename cols
subjectDiagnoses = subjectDiagnoses.filter(subjectDiagnoses.ICD9_CODE != \
                                           "ICD9_CODE") # remove first line with "SUBJECT_ID" value
subjectDiagnoses.show(5)

# COMMAND ----------

#chartEvents.groupBy("_c1").count().show()
chartEventsBySubject = chartEvents1.select(col("_c1").alias("SUBJECT_ID"), col("_c4").alias("CHARTCD"), \
                        col("_c9").alias("VALUENUM"), col("_c10").alias("VALUEUOM"),col("_c11").alias("WARNING"))
# remove first line with "SUBJECT_ID" and other headers
chartEventsBySubject = chartEventsBySubject.filter(chartEventsBySubject.SUBJECT_ID != "SUBJECT_ID")
chartEventsBySubject.show(5)

# COMMAND ----------

chartEventsWarningBySubject = chartEventsBySubject.filter("WARNING == 1")   # only where WARNING = 1
chartEventsWarningBySubject.show(5)

# COMMAND ----------

labEvents1.groupBy("_c1").count().show(5)  # lab event counts per subject (_c1) sampling
"""
+-----+-----+
|  _c1|count|
+-----+-----+
|41983|  125|
|10114|  616|
|10088| 1112|
|43735|  408|
|40456|  155|
+-----+-----+
only showing top 5 rows
"""

# COMMAND ----------

labEventsBySubject = labEvents1.select(col("_c1").alias("SUBJECT_ID"), col("_c3").alias("LABCD"), \
                                      col("_c5").alias("VALUE"), col("_c6").alias("VALUENUM")\
                                      , col("_c7").alias("VALUEUOM"), col("_c8").alias("FLAG"))
labEventsBySubject = labEventsBySubject.filter(labEventsBySubject.FLAG.contains("abnormal")) # abnormal only
labEventsBySubject = labEventsBySubject.drop("VALUE", "FLAG")  # filter out normal  readings trim column
labEventsBySubject.show(5)

"""
+----------+-----+--------+--------+
|SUBJECT_ID|LABCD|VALUENUM|VALUEUOM|
+----------+-----+--------+--------+
|     10120|51221|    23.9|       %|
|     10120|51222|     8.9|    g/dL|
|     10120|51237|     1.7|    null|
|     10120|51248|    26.8|      pg|
|     10120|51249|    37.4|       %|
+----------+-----+--------+--------+
only showing top 5 rows
"""

# COMMAND ----------

subjectDiagnoses.take(4)

# COMMAND ----------

diagnosesBySubjectRDD = subjectDiagnoses.rdd  # convert spark DF to RDD for map reduce by key
diagnosesBySubjectRDD.take(3)
diagnosesBySubjectRDD.keys().take(5)


# COMMAND ----------

diagnosesBySubjectRDD.values().take(5)

# COMMAND ----------

mappedSubjectDiagnosis = diagnosesBySubjectRDD.map(lambda rec: (rec[0], rec[1])) # subject and ICD by key pairs
mappedSubjectDiagnosis.take(3)

# COMMAND ----------

reducedSubjectDiagnosis = mappedSubjectDiagnosis.reduceByKey(lambda subject, note: subject + " " + note)
reducedSubjectDiagnosis.take(5)     # gather ICD concepts by subject key

# COMMAND ----------

# look for admissions of patients with existing critcal DIAGNOSIS, rename SUBJECT_ID to SUBJECT_ID2 to remove dup later
admissions2 = admissions1.select(col("_c1").alias("SUBJECT_ID2"), \
  col("_c2").alias("HADM_ID"),\
  col("_c3").alias("ADMITTIME"),\
  col("_c4").alias("DISCHTIME"),\
  col("_c5").alias("DEATHTIME"),\
  col("_c6").alias("ADMISSION_TYPE"),\
  col("_c9").alias("INSURANCE"),\
  col("_c16").alias("DIAGNOSIS"))

admissions2 = admissions2.filter(col("SUBJECT_ID2") != "SUBJECT_ID")  # remove 1st line


admitsByDiagnosis = admissions2.groupBy("DIAGNOSIS").count() 
# count Diagnoses ICD  per subject
admitsPeek = admitsByDiagnosis.orderBy("count", ascending=False)  # show occurances Diagnosis 
#  admitsPeek.show()
"""
+--------------------+-----+
|           DIAGNOSIS|count|
+--------------------+-----+
|              SEPSIS|   10|
|           PNEUMONIA|    8|
| SHORTNESS OF BREATH|    4|
|               FEVER|    4|
|   FAILURE TO THRIVE|    3|
|CONGESTIVE HEART ...|    3|
|          STROKE/TIA|    2|
|      UPPER GI BLEED|    2|
|   ESOPHAGEAL CA/SDA|    2|
|GASTROINTESTINAL ...|    2|
|       LIVER FAILURE|    2|
|RESPIRATORY DISTRESS|    2|
|         HYPOTENSION|    2|
|ASTHMA;CHRONIC OB...|    2|
"""

# COMMAND ----------

admissions2 = admissions2.withColumn("DISCHTIME", datediff(admissions2.DISCHTIME, admissions2.ADMITTIME))
admissions2 = admissions2.withColumnRenamed("DISCHTIME", "LOS")   # length of stay in days

admissions2.show(5)


# COMMAND ----------

patients2 = patients1.select(col("_c1").alias("SUBJECT_ID"),\
                            col("_c2").alias("GENDER"),\
                             col("_c3").alias("DOB"),\
                             col("_c4").alias("DOD"),\
                             col("_c5").alias("DOD_HOSP"))

patients2 = patients2.filter(col("SUBJECT_ID") != "SUBJECT_ID") # trim first row
#patients2 = patients2.filter(col("DOD_HOSP").isNotNull()) 

patients2 = patients2.select(col("SUBJECT_ID"), col("GENDER"), \
                       datediff(patients2.DOD, patients2.DOB).alias('AGE'))
patients2 = patients2.withColumn("AGE", round(col("AGE")/ 365.0, 1))
#round(col('average'), 3
  
patients2.show(5)

# COMMAND ----------

# MAGIC 

# COMMAND ----------

patientsInfo2 = patients2.join(admissions2, patients2.SUBJECT_ID == admissions2.SUBJECT_ID2, 'inner')
patientsInfo2 = patientsInfo2.filter(col("DEATHTIME").isNotNull())   # 
patientsInfo2 = patientsInfo2.drop("SUBJECT_ID2")
if 0:     # get only the Sepsis patients
  patientsSepsis2 = patientsInfo2.filter(col("DIAGNOSIS") == "SEPSIS")

patientsInfo2.show(4)



# COMMAND ----------

labEvents1.show(4)
"""
+-------+----------+-------+------+-------------------+-----+--------+--------+--------+
|    _c0|       _c1|    _c2|   _c3|                _c4|  _c5|     _c6|     _c7|     _c8|
+-------+----------+-------+------+-------------------+-----+--------+--------+--------+
| ROW_ID|SUBJECT_ID|HADM_ID|ITEMID|          CHARTTIME|VALUE|VALUENUM|VALUEUOM|    FLAG|
|6294375|     10120| 193924| 51214|2115-05-13 14:53:00|  226|     226|   mg/dL|    null
"""


# COMMAND ----------

labEventsPeek = labEvents1.groupBy("_c3")
labEventsPeek.count().show()





# COMMAND ----------

patients3 = patients1.select(col("_c1").alias("SUBJECT_ID"),\
                            col("_c2").alias("GENDER"),\
                             col("_c3").alias("DOB"),\
                             col("_c4").alias("DOD"),\
                             col("_c5").alias("DOD_HOSP"))


patients3 = patients3.filter(col("SUBJECT_ID") != "SUBJECT_ID") # trim first row

# patients3 = patients3.filter(col("DOD_HOSP").isNotNull()) 
#patients3.withColumn("TARGET",F.when(col("DOD_HOSP").isNull(), "N").otherwise("Y"))

patients3 = patients3.select(col("SUBJECT_ID"), col("GENDER"), \
                       datediff(patients3.DOD, patients3.DOB).alias('AGE'), \
                            col("DOD_HOSP").alias("TARGET"))
patients3 = patients3.withColumn("AGE", round(col("AGE")/ 365.0, 1))
patients3 = patients3.withColumn("TARGET", F.when(col("TARGET").isNull(), "N").otherwise("Y"))
  
patients3.show(5)

# join patients and admissions
# remove extra SUBJECT_ID column and DEATHTIME since it matches Discharge date

patientsInfo2 = patients3.join(admissions2, patients3.SUBJECT_ID == admissions2.SUBJECT_ID2, 'inner')
# patientsInfo2 = patientsInfo2.filter(col("DEATHTIME").isNotNull()) 
patientsInfo2 = patientsInfo2.drop("SUBJECT_ID2", "DEATHTIME")

if 0:     # don't filter to Sepsis patients just yet
  patientsSepsis2 = patientsInfo2.filter(col("DIAGNOSIS") == "SEPSIS")

patientsInfo2.show(5)
"""   patient mortalities in hospital (TARGET == Y if true)
+----------+------+----+------+-------+-------------------+---+--------------+---------+-------------------+
|SUBJECT_ID|GENDER| AGE|TARGET|HADM_ID|          ADMITTIME|LOS|ADMISSION_TYPE|INSURANCE|          DIAGNOSIS|
+----------+------+----+------+-------+-------------------+---+--------------+---------+-------------------+
|     10006|     F|71.5|     Y| 142345|2164-10-23 21:09:00|  9|     EMERGENCY| Medicare|             SEPSIS|
|     10011|     F|36.3|     Y| 105331|2126-08-14 22:32:00| 14|     EMERGENCY|  Private|        HEPATITIS B|
|     10013|     F|87.2|     Y| 165520|2125-10-04 23:36:00|  3|     EMERGENCY| Medicare|             SEPSIS|
|     10017|     F|77.0|     N| 199207|2149-05-26 17:19:00|  8|     EMERGENCY| Medicare|   HUMERAL FRACTURE|
|     10019|     M|48.9|     Y| 177759|2163-05-14 20:43:00|  1|     EMERGENCY| Medicare|ALCOHOLIC HEPATITIS|
+----------+------+----+------+-------+-------------------+---+--------------+---------+-------------------+
only showing top 5 rows
"""
patientsInfo2.groupBy(col("SUBJECT_ID")).count().show()  # some overlapping admissions for same subject
"""
|SUBJECT_ID|count|
+----------+-----+
|     41983|    1|
|     10114|    1|
|     10088|    3|
|     43735|    1|
|     40456|    1|
|     10124|    2|
"""
# clean the directory if needed hdfs dfs -rm -r /user/cottrell_ken/data/output/*
# writes intermediate hdfs dfs -ls /user/cottrell_ken/data/output/patientInfo.csv
intermediatePatientInfoFile = "data/output/" + myTimeStamp + "patientsInfo2.csv"
patientsInfo2.write.csv(intermediatePatientInfoFile)
labEventsBySubject.show(5)
"""
+----------+-----+--------+--------+
|SUBJECT_ID|LABCD|VALUENUM|VALUEUOM|
+----------+-----+--------+--------+
|     10120|51221|    23.9|       %|
|     10120|51222|     8.9|    g/dL|
|     10120|51237|     1.7|    null|
|     10120|51248|    26.8|      pg|
|     10120|51249|    37.4|       %|
+----------+-----+--------+--------+
only showing top 5 rows
"""



sepsisRiskMarkersDF = sc.parallelize([sepsisRiskMarkersDict])
sepsisRiskMarkersDF.count()



# COMMAND ----------

sepsisRiskMarkersDict.has_key('55512')

# COMMAND ----------


""" labEvents1.show()
+-------+----------+-------+------+-------------------+-----+--------+--------+--------+
|    _c0|       _c1|    _c2|   _c3|                _c4|  _c5|     _c6|     _c7|     _c8|
+-------+----------+-------+------+-------------------+-----+--------+--------+--------+
| ROW_ID|SUBJECT_ID|HADM_ID|ITEMID|          CHARTTIME|VALUE|VALUENUM|VALUEUOM|    FLAG|
|6294375|     10120| 193924| 51214|2115-05-13 14:53:00|  226|     226|   mg/dL|    null
"""

labEvents2 = labEvents1.select(col("_c1").alias("SUBJECT_ID"), \
                               col("_c2").alias("HADM_ID"), \
                               col("_c3").alias("ITEMID"), \
                               col("_c4").alias("CHARTIME"), \
                                 col("_c5").alias("VALUE"), \
                               col("_c6").alias("VALUENUM"), \
                               col("_c7").alias("VALUEUOM"), \
                               col("_c8").alias("FLAG"))
labEvents2 = labEvents2.filter(col("SUBJECT_ID") != "SUBJECT_ID")
labEvents2 = labEvents2.filter(col("FLAG").isNotNull())
labEvents2 = labEvents2.drop(col("FLAG"))    # restrict to abnormal readings
"""labEvents2.show()
+----------+-------+------+-------------------+-----+--------+--------+
|SUBJECT_ID|HADM_ID|ITEMID|           CHARTIME|VALUE|VALUENUM|VALUEUOM|
+----------+-------+------+-------------------+-----+--------+--------+
|     10120| 193924| 51221|2115-05-13 14:53:00| 23.9|    23.9|       %|
|     10120| 193924| 51222|2115-05-13 14:53:00|  8.9|     8.9|    g/dL|
|     10120| 193924| 51237|2115-05-13 14:53:00|  1.7|     1.7|    null|
|     10120| 193924| 51248|2115-05-13 14:53:00| 26.8|    26.8|      pg|
"""

# COMMAND ----------

# now have to filter out those ITEMIDS not in the sepsisMarkersDict

sepsisCodeSet = sepsisRiskMarkersDict.keys()
sepsisCodeSet = [50816,50818,50820,50821,50825,50827,50956,51221,51222,51237,51244,50863,51248,50867,51256,51006,\
                 51265,50882,50889,51274,51275,51277,50893,51288,50912,51300,50931,50878,50813]
def isSepsisRiskCode(sepsisCodeSet, thisCode):
  # sepsisCodeList = sepsisCodes
  if thisCode in sepsisCodeSet:
    return True
  else:
    return False

# COMMAND ----------

isSepsisRiskCode(sepsisCodeSet, 50818)   # make sure to use NNNNN not 'NNNNN'

# COMMAND ----------

labSubjectItems = labEvents2.select(col("SUBJECT_ID"), col("ITEMID"))
labSubjectItemsRDD = labSubjectItems.rdd
"""labSubjectItems.take(3)
[Row(SUBJECT_ID=u'10120', ITEMID=u'51221'),
 Row(SUBJECT_ID=u'10120', ITEMID=u'51222'),
 Row(SUBJECT_ID=u'10120', ITEMID=u'51237')]
"""

# need to filter the long list of ITEMIDs for each SUBJECT_ID / HADM_ID into just those from the sepsisMarketDict set!


labSubjectItemsRDDmapped = labSubjectItemsRDD.map(lambda rec: (rec[0], rec[1])) # SUBJECT_ID and ITEMCODE by key pairs
# first collect all the ITEMIDs for each SUBJECT_ID
labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduceByKey(lambda subjectid, labitemid: subjectid + " " + labitemid)

#labSubjectItemsRDDreduced.take(1)
#[(u'10006',
#  u'50813 50804 50818 50820 50882 50912 50931 50954 50960 50970 50971 51493 51516 50862 50893 50902 50910 50912 50931 50960 #50971 51221 51222 51274 51277 51279 50882 50893 50912 50931 50970 50893 50910 50912 50970 51221 51222 51244 51256 51516 50912 #50931 50970 51006 51221 51222 51274 51277

# now remove the ones not inthe sepsis code set
labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduceByKey(lambda subjectid, labitemid: \
                                          subjectid + " " + labitemid if labitemid in sepsisCodeSet else subjectid)

#labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduce(lambda rec: (rec[0], rec[1]) \
#                                                            if rec[1] in sepsisCodeSet else rec[0] )

# COMMAND ----------
# COMMAND ----------

labSubjectItemsRDDmapped = labSubjectItemsRDD.map(lambda rec: (rec[0], rec[1])) # SUBJECT_ID and ITEMCODE by key pairs
labs2 = labSubjectItemsRDD.filter(lambda record: record[1] not in sepsisCodeSet)
labs2.count()

# COMMAND ----------

labSubjectItemsRDDmapped = labSubjectItemsRDD.map(lambda rec: (rec[0], rec[1])) # SUBJECT_ID and ITEMCODE by key pairs
# first collect all the ITEMIDs for each SUBJECT_ID
labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduceByKey(lambda subjectid, labitemid: subjectid + " " + labitemid)

#labSubjectItemsRDDreduced.take(1)
#[(u'10006',
#  u'50813 50804 50818 50820 50882 50912 50931 50954 50960 50970 50971 51493 51516 50862 50893 50902 50910 50912 50931 50960 #50971 51221 51222 51274 51277 51279 50882 50893 50912 50931 50970 50893 50910 50912 50970 51221 51222 51244 51256 51516 50912 #50931 50970 51006 51221 51222 51274 51277

# now remove the ones not inthe sepsis code set
labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduceByKey(lambda subjectid, labitemid: \
                                          subjectid + " " + labitemid if labitemid in sepsisCodeSet else subjectid)

#labSubjectItemsRDDreduced = labSubjectItemsRDDmapped.reduce(lambda rec: (rec[0], rec[1]) \
#                                                            if rec[1] in sepsisCodeSet else rec[0] )

sepsisCodeSet = sepsisRiskMarkersDict.keys()
#sepsisCodeSet = ['50893', '50878', '50956', '51300', '51244', '51248', '51265', '50820', '50821', '50827', '50825', '51222', '51221', '50882',  #'50889', '50818', '51288', '50863', '50816', '50867', '51237', '51256', '51274', '51275', '51277', '50912', '51006', '50931','50813']

schemaString = "ITEMCODE"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

sepsisCodesRDD = sc.parallelize(sepsisCodeSet)
row_rdd = sepsisCodesRDD.map(lambda x: Row(x))
sepsisCodesTable =spark.createDataFrame(row_rdd,schema)


# COMMAND ----------

labSubjectSepsisItems = sepsisCodesTable.join(labEvents2, sepsisCodesTable.ITEMCODE == labEvents2.ITEMID, 'inner')
labSubjectSepsisItems.show(5)

# COMMAND ----------

labSubjectSepsisItems = labSubjectSepsisItems.drop("ITEMCODE", "HADM_ID", "VALUENUM", "VALUEUOM")
labSubjectSepsisItems.show(5)

# COMMAND ----------

subjectCounts = labSubjectSepsisItems.groupBy("SUBJECT_ID").count()   # count subjects with a Sepsis-related itemid 
itemCounts = labSubjectSepsisItems.groupBy("ITEMID").count()


# COMMAND ----------

subjectCounts.orderBy("count", ascending=False).show(5)

# COMMAND ----------

itemCounts.orderBy("count", ascending=False).show(29)   # show all ITEMIDS present (shows 26 out of 29) 

# COMMAND ----------

#subjectsTop20 = diagnoses1.groupBy("_c1").count()    # count Diagnoses ICD  per subject
#subjectsTop20 = subjectsTop20.orderBy("count", ascending=False)  # show subjects with longest list of ICD
#subjectsTop20 = subjectsTop20.select(col("_c1").alias("SUBJECT_ID"), col("count"))

# COMMAND ----------



# COMMAND ----------

labSubjectSepsisItems.count()




patientsInfo2.count()   # how many patients total in data set in hosp

# COMMAND ----------

patientsInfo2.show(5)

# COMMAND ----------

patientsInfoAllDiagnoses = patientsInfo2
patientsInfoSepsis = patientsInfo2.filter(col("DIAGNOSIS") == "SEPSIS")
# patientsInfoSepsis.count()  # only ~ 10 patients with Sepsis on admission in demo data set
# patientsInfoAllDiagnoses.count()   # about 129 patients with all diagnoses ...

# COMMAND ----------

labSubjectSepsisItems.show(5)

# COMMAND ----------



# COMMAND ----------

temptable = labSubjectSepsisItems.groupBy(col("SUBJECT_ID"), col("ITEMID"))

# COMMAND ----------

tempcrosstab = labSubjectSepsisItems.crosstab("SUBJECT_ID", "ITEMID")

# COMMAND ----------

tempcube = labSubjectSepsisItems.cube("SUBJECT_ID", "ITEMID").count().orderBy("SUBJECT_ID", "ITEMID")
tempcube = tempcube.select(col("SUBJECT_ID"), col("ITEMID"), col("count")).filter(col("SUBJECT_ID").isNotNull())


# COMMAND ----------

#tempcube.show()
#+----------+------+-----+
#|SUBJECT_ID|ITEMID|count|
#+----------+------+-----+
#|     10006|  null|  566|
#|     10006| 50813|    6|
#|     10006| 50818|    1|
# temptable.count().show()
#+----------+------+-----+
#|SUBJECT_ID|ITEMID|count|
#+----------+------+-----+
#|     10017| 50821|    2|
#|     42458| 51222|    6|
#|     42281| 51274|   25|
#|     10088| 51274|    2|
# tempcrosstab.show()
#+-----------------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
#|SUBJECT_ID_ITEMID|50813|50818|50820|50821|50863|50867|50878|50882|50889|50893|50912|50931|50956|51006|51221|51222|51237|51244|51248|51256|51265|51274|51275|51277|51288|51300|
#+-----------------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
#|            43746|    1|    1|    0|    1|   14|    0|   14|   12|    0|   20|   13|   11|    0|   20|   19|   19|    6|    2|    1|    2|    1|    6|    2|   19|    0|    0|
#|            42458|    1|    0|    0|    0|    0|    0|    0|    0|    0|    


# COMMAND ----------

# tempcrosstab.select(col("SUBJECT_ID_ITEMID")).sort("SUBJECT_ID_ITEMID", ascending=False).show(200)
#+-----------------+
#|SUBJECT_ID_ITEMID|
#+-----------------+
#|            44228|
#|            44222|
#|            44212|

# COMMAND ----------

SubjectLabDataMatrix = labSubjectSepsisItems.crosstab("SUBJECT_ID", "ITEMID")
# SubjectLabDataMatrix.show()   # creates column SUBJECT_ID_ITEMID

PatientSepsisInfoTrainingData = patientsInfoSepsis.join(SubjectLabDataMatrix, \
                         patientsInfoSepsis.SUBJECT_ID == SubjectLabDataMatrix.SUBJECT_ID_ITEMID, 'inner' )

# COMMAND ----------

print (PatientSepsisInfoTrainingData.columns)    # the list of Sepsis-related Lab Item IDs used

# COMMAND ----------

# omit these columns   'ADMISSION_TYPE', 'INSURANCE', 'DIAGNOSIS', 'SUBJECT_ID_ITEMID'

PatientLabIDCounts = PatientSepsisInfoTrainingData.select('SUBJECT_ID', 'GENDER', 'AGE', 'TARGET', 'LOS', '50813', '50818', '50820', '50821', '50863', '50867', '50878', '50882', '50889', '50893', '50912', '50931', '50956', '51006', '51221', '51222', '51237', '51244', '51248', '51256', '51265', '51274', '51275', '51277', '51288', '51300')
PatientLabIDCounts.show()

# COMMAND ----------

print(PatientLabIDCounts)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

ignore = ['SUBJECT_ID', 'TARGET', 'GENDER']
assembler = VectorAssembler(
    inputCols=[x for x in PatientLabIDCounts.columns if x not in ignore],
    outputCol='features')

assembler.transform(PatientLabIDCounts)

# COMMAND ----------

print (assembler.explainParams())
if 0:
  from pyspark.ml import Pipeline
  pipeline = Pipeline(stages=[assembler, kmeans_estimator])
  model = Pipeline.fit(PatientLabIDCounts)

# COMMAND ----------

# use sepsisCodeSet as columns, for some reason can't resolve columns 50827, 50825, 50816 - not sure why , 
# print(sepsisCodeSet)
# ['50893', '50878', '50956', '51300', '51244', '51248', '51265', 
#'50820', '50821', '50827', '50825', '51222', '51221', '50882', '50889', 
# '50818', '51288', '50863', '50816', '50867', '51237', '51256', '51274',
# '51275', '51277', '50912', '51006', '50931', '50813']


testdf = PatientLabIDCounts.groupBy().avg('50893', '50878', '50956', '51300', '51244', '51248', '51265', '50820', '50821', '51222', '51221', '50882', '50889', '50818', '51288', '50863', '50867', '51237', '51256', '51274', '51275', '51277', '50912', '51006', '50931', '50813')
testdf.show()

bioEvents2 = bioEvents1.select(col("_c1").alias("SUBJECT_ID2"), \
                               col("_c2").alias("HADM_ID"), \
                               col("_c3").alias("CHARTDATE"), \
                               col("_c4").alias("CHARTIME"), \
                                 col("_c5").alias("SPEC_ITEMID"), \
                               col("_c6").alias("SPEC_TYPE_DESC"), \
                               col("_c7").alias("ORG_ITEMID"), \
                               col("_c8").alias("ORG_NAME"), \
                              col("_c9").alias("ISOLATE_NUM"), \
                               col("_c10").alias("AB_ITEMID"), \
                                 col("_c11").alias("AB_NAME"), \
                                col("_c12").alias("DILUTION_TEXT"), \
                                col("_c13").alias("DILUTION_COMPARISON"), \
                                col("_c14").alias("DILUTION_VALUE"), \
                                col("_c15").alias("INTERPRETATION"))

bioEvents2 = bioEvents2.filter(col("SUBJECT_ID2") != "SUBJECT_ID")
#bioEvents2 = bioEvents2.filter(col("FLAG").isNotNull())
#bioEvents2 = bioEvents2.drop(col("FLAG"))    # restrict to abnormal

PatientsSepsisList = PatientLabIDCounts.select(col("SUBJECT_ID"))

PatientsSepsisLabsBioEvents = \
   PatientsSepsisList.join(bioEvents2, bioEvents2.SUBJECT_ID2 == PatientsSepsisList.SUBJECT_ID, 'inner')

PatientsSepsisLabsBioEvents.drop("SUBJECT_ID2")
PatientsSepsisLabsBioEvents.columns

diagICDpeek = diagICDLookup1.select(col("_c1").alias("ICD9_CODE"),\
                                     col("_c2").alias("SHORT_TITLE"))
diagICDpeek.show(5)

expression = "epsis"

tempdf = diagICDpeek.select(col("SHORT_TITLE").rlike(expression))
tempdf.show()



# COMMAND ----------

subjectDiagnoses.columns

# COMMAND ----------

diagnosesBySubjectRDD.count()

# COMMAND ----------

diagnoses1.show()

# COMMAND ----------

diagnoses2 = diagnoses1.select(col("_c1").alias("SUBJECT_ID2"), col("_c4").alias("ICD9_CODE"))

# JOIN entire Diagnsis list  with only Sepsis-admitted patient from PatientsSepsisList
PatientsSepsisList.show()
diagnosesSepsisOnly = \
   PatientsSepsisList.join(diagnoses2, diagnoses2.SUBJECT_ID2 == PatientsSepsisList.SUBJECT_ID, 'inner')

diagnosesSepsisOnly.drop("SUBJECT_ID2")
diagnosesSepsisOnly.columns   # why doesn't drop work?

# COMMAND ----------

#diagnoses2.count()
diagnosesSepsisOnly.select("SUBJECT_ID", "ICD9_CODE").show()  # why doesn't drop("SUBJECT_ID2") work?


# COMMAND ----------

PatientsTrainingDataADMLAB = PatientLabIDCounts      # Patient accumulated admit, LOS, lab codes (only for for Sepsis = 29 columns)
PatientDiagnosisMatrix = diagnoses2.crosstab("SUBJECT_ID2", "ICD9_CODE")   # very sparse matrix ; can't figure out which codes are for Sepsis ICD9 only
# print(PatientDiagnosisMatrix.columns)
# PatientDiagnosisMatrix.show(10)

# COMMAND ----------

print (PatientDiagnosisMatrix.columns)

# COMMAND ----------

# next look through these codes for sepsis risk markers
ICDcodesDescription = diagICDLookup1.select(col("_c1").alias("ICD9_CODE"),\
                                     col("_c2").alias("SHORT_TITLE"))
#ICDcodesDescription.count()   # 15568  ICD9-CODE - TITLE pairs, so which ones can be tied to sepsis risk factors?


# COMMAND ----------

# look for "epsis", "SIRS", "SOFA", "SAP"
sepsisICD9codes = ICDcodesDescription.filter(ICDcodesDescription.SHORT_TITLE.contains("epsis") |  \
                                            ICDcodesDescription.SHORT_TITLE.contains("SIRS"))
sepsisICD9codes.show()

# COMMAND ----------

print(sepsisRiskMarkersDict.keys())

# COMMAND ----------

ICDcodesDescription.filter(ICDcodesDescription.ICD9_CODE.contains('50813')).show()

# COMMAND ----------

sepsisCodesTable.show()   # this is RDD of the sepsisRiskMarkersDict {} 


# COMMAND ----------

# convert sepsis Dictionary to table and do join with Diagnoses 

joinedSepsisDictPatientDiagnoses = sepsisCodesTable.join(ICDcodesDescription, ICDcodesDescription.ICD9_CODE == sepsisCodesTable.ITEMCODE, 'inner')
joinedSepsisDictPatientDiagnoses.count()    # this returns zero, so MIMIC Diagnoses codes don't show up in Sepsis codes dictionary 


# COMMAND ----------

patientsInfoAllDiagnoses.count()    # now let's check the broader set (not just Diagnosis = Sepsis on admit)

# COMMAND ----------

patientsInfoAllDiagnoses.show(5)


# COMMAND ----------

sepsisICD9codes.show(5)

# COMMAND ----------


diagnoses2.show(4)

# COMMAND ----------

# look for patients SUBJECT_IDs  that have any of these additional codes from sepsisICD9codes table
#|     77181|NB septicemia [se...|
#|    99590|           SIRS, NOS|
#|    99591|              Sepsis|
#|    99592|       Severe sepsis|
#|    99593

diagnoses2 = diagnoses2.withColumnRenamed("ICD9_CODE", "ICD9_CODE2")
diagnoses3 = diagnoses2.join(sepsisICD9codes, sepsisICD9codes.ICD9_CODE == diagnoses2.ICD9_CODE2, 'inner')
diagnoses3.count()

# COMMAND ----------

diagnoses3.show()

# COMMAND ----------

# so all of this needs to be redone
###############

# create a single subject / condition code table from both explicit sepsis dictionary and MIMIC implicit (found)  codes
subjectConditionCodeTable1 = labSubjectSepsisItems.select(col("SUBJECT_ID"), col("ITEMID").alias("XCODE"))    # 18723 records
subjectConditionCodeTable2 = diagnoses3.select(col("SUBJECT_ID2").alias("SUBJECT_ID"), col("ICD9_CODE2").alias("XCODE"))   # 35  records
# subjectConditionCodeTable2.count()




# COMMAND ----------



# COMMAND ----------

subjectConditionTableTotal = subjectConditionCodeTable1.union(subjectConditionCodeTable2)
subjectConditionTableTotal.count()   # 18578 rows (non unique)


# COMMAND ----------


  # this is additional Patients with Sepsis related ICD9 codes diagnosed after admit, during LOS
  # XCODE is union of  (external) sepsis ditionary key terms '5NNNN' and ICD9 CODES from MIMIC patient diagnoses table
SubjectConditionCodeCrossTab = subjectConditionTableTotal.crosstab("SUBJECT_ID", "XCODE")



 

# COMMAND ----------

SubjectConditionCodeCrossTab.show()

# COMMAND ----------

PatientSepsisInfoTrainingData = patientsInfoSepsis.join(SubjectConditionCodeCrossTab, \
                         patientsInfoSepsis.SUBJECT_ID == SubjectConditionCodeCrossTab.SUBJECT_ID_XCODE, 'inner' )

PatientsInfoAllDiagnosesTrainingData = patientsInfoAllDiagnoses.join(SubjectConditionCodeCrossTab, \
                                  patientsInfoAllDiagnoses.SUBJECT_ID == SubjectConditionCodeCrossTab.SUBJECT_ID_XCODE, 'inner')

# COMMAND ----------

PatientSepsisInfoTrainingData.count()   # 10 records with SEPSIS on first ADT 
PatientsInfoAllDiagnosesTrainingData.count() # 129 records with Sepsis either on ADT diagnosis or sepsis-related Diagnoses during LOS

# COMMAND ----------

PatientsInfoAllDiagnosesTrainingData.show()

# COMMAND ----------
#   ****** ML PySpark code added Sep 6 2018 *****
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression


def yes_no_one_zero(x):
  if x == 'Y':
    return float(1)
  elif x == 'N':
    return float(0)
  else :
    return float (0)


# PatientSepsisInfoTrainingData.count()  # 1183 records in full data set (versus only 10 with demo)
pSITD = PatientSepsisInfoTrainingData  # abreviation
pIADD = PatientsInfoAllDiagnosesTrainingData
 
func_udf = udf(yes_no_one_zero, StringType())
pSITD = pSITD.withColumn('LABEL',func_udf(pSITD['TARGET']))   # replace TARGET Y/N with LABEL = 1/0
pIADD = pIADD.withColumn('LABEL',func_udf(pIADD['TARGET']))   # replace TARGET Y/N with LABEL = 1/0


pSITD = pSITD.withColumn("LABEL", pSITD['LABEL'].cast(DoubleType()))
pIADD = pIADD.withColumn("LABEL", pIADD['LABEL'].cast(DoubleType()))

pIADD.schema


# COMMAND ----------





# <<START-EDIT  180910  too many diagnoses for too many feature columns
#   in demo data set works ok but not for full patient DIAGNOSES file
#   so from demo diagnoses just use these columns in a clipped 
#   Patient info file "pIADD"

demodiagfeatures = ['SUBJECT_ID_DIAGNOSIS',' MITRAL REGURGITATION;CORONARY ARTERY DISEASE\\CORONARY ARTERY BYPASS GRAFT WITH MVR  ? MITRAL VALVE REPLACEMENT /SDA', 'ABDOMINAL PAIN', 'ABSCESS', 'ACUTE CHOLANGITIS', 'ACUTE CHOLECYSTITIS', 'ACUTE PULMONARY EMBOLISM', 'ACUTE RESPIRATORY DISTRESS SYNDROME;ACUTE RENAL FAILURE', 'ACUTE SUBDURAL HEMATOMA', 'ALCOHOLIC HEPATITIS', 'ALTERED MENTAL STATUS', 'AROMEGLEY;BURKITTS LYMPHOMA', 'ASTHMA/COPD FLARE', 'ASTHMA;CHRONIC OBST PULM DISEASE', 'BASAL GANGLIN BLEED', 'BRADYCARDIA', 'BRAIN METASTASES', 'CELLULITIS', 'CEREBROVASCULAR ACCIDENT', 'CHEST PAIN', 'CHEST PAIN/ CATH', 'CHOLANGITIS', 'CHOLECYSTITIS', 'CHRONIC MYELOGENOUS LEUKEMIA;TRANSFUSION REACTION', 'CONGESTIVE HEART FAILURE', 'CORONARY ARTERY DISEASE\\CORONARY ARTERY BYPASS GRAFT /SDA', 'CRITICAL AORTIC STENOSIS/HYPOTENSION', 'ELEVATED LIVER FUNCTIONS;S/P LIVER TRANSPLANT', 'ESOPHAGEAL CA/SDA', 'ESOPHAGEAL CANCER/SDA', 'FACIAL NUMBNESS', 'FAILURE TO THRIVE', 'FEVER', 'FEVER;URINARY TRACT INFECTION', 'GASTROINTESTINAL BLEED', 'HEADACHE', 'HEPATIC ENCEP', 'HEPATITIS B', 'HUMERAL FRACTURE', 'HYPOGLYCEMIA', 'HYPONATREMIA;URINARY TRACT INFECTION', 'HYPOTENSION', 'HYPOTENSION, RENAL FAILURE', 'HYPOTENSION;TELEMETRY', 'HYPOTENSION;UNRESPONSIVE', 'INFERIOR MYOCARDIAL INFARCTION\\CATH', 'LEFT HIP FRACTURE', 'LEFT HIP OA/SDA', 'LIVER FAILURE', 'LOWER GI BLEED', 'LUNG CANCER;SHORTNESS OF BREATH', 'MEDIASTINAL ADENOPATHY', 'METASTATIC MELANOMA;BRAIN METASTASIS', 'METASTIC MELANOMA;ANEMIA', 'MI CHF', 'NON SMALL CELL CANCER;HYPOXIA', 'OVERDOSE', 'PERICARDIAL EFFUSION', 'PLEURAL EFFUSION', 'PNEUMONIA', 'PNEUMONIA/HYPOGLCEMIA/SYNCOPE', 'PNEUMONIA;TELEMETRY', 'PULMONARY EDEMA, MI', 'PULMONARY EDEMA\\CATH', 'RECURRENT LEFT CAROTID STENOSIS,PRE HYDRATION', 'RENAL CANCER/SDA', 'RENAL FAILIURE-SYNCOPE-HYPERKALEMIA', 'RESPIRATORY DISTRESS', 'RIGHT HUMEROUS FRACTURE', 'S/P FALL', 'S/P MOTOR VEHICLE ACCIDENT', 'S/P MOTORCYCLE ACCIDENT', 'SEIZURE', 'SEIZURE;STATUS EPILEPTICUS', 'SEPSIS', 'SEPSIS; UTI', 'SEPSIS;PNEUMONIA;TELEMETRY', 'SEPSIS;TELEMETRY', 'SHORTNESS OF BREATH', 'STATUS POST MOTOR VEHICLE ACCIDENT WITH INJURIES', 'STEMI;', 'STROKE/TIA', 'SUBDURAL HEMATOMA/S/P FALL', 'SYNCOPE;TELEMETRY', 'SYNCOPE;TELEMETRY;INTRACRANIAL HEMORRHAGE', 'TACHYPNEA;TELEMETRY', 'TRACHEAL ESOPHAGEAL FISTULA', 'TRACHEAL STENOSIS', 'UNSTABLE ANGINA', 'UPPER GI BLEED', 'URINARY TRACT INFECTION;PYELONEPHRITIS', 'UROSEPSIS', 'UTI/PYELONEPHRITIS', 'VARICEAL BLEED', 'VF ARREST ', 'VOLVULUS']
if 0:
	demodiagfeaturesRDD = sc.parallelize(demodiagfeatures)
	schemaString = "DIAGNOSES2"
	fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
	schema = StructType(fields)
	row_rdd = demodiagRDD.map(lambda x: Row(x))
	demoDiagColumn =spark.createDataFrame(row_rdd,schema)
	fewerDiagTable  = demoDiagColumn.join(pIADD,\
                      pIADD.DIAGNOSIS == demodiagColumn.DIAGNOSIS2, 'inner')

pIADD = fewerDiagTable.drop("DIAGNOSES2")

#   end of  patch to full MIMIC patient diagnses file to limit CrossTab columns
# 180910 END-EDIT>>

# df.crosstab in order to xform categoricals into vectors
patientsGenderCrosstab = pIADD.crosstab("SUBJECT_ID", "GENDER")
patientsAdmitTypeCrossTab = pIADD.crosstab("SUBJECT_ID", "ADMISSION_TYPE")
patientsInsuranceTypeCrossTab = pIADD.crosstab("SUBJECT_ID", "INSURANCE")
patientsADTdiagnosisCrossTab = pIADD.crosstab("SUBJECT_ID", "DIAGNOSIS")
# PatientSepsisInfoTrainingData.describe().show()

# COMMAND ----------

#   SUBJECT_ID_GENDER,   SUBJECT_ID_ADMISSION_TYPE,   SUBJECT_ID_INSURANCE, SUBJECT_ID_DIAGNOSIS
pIADD = pIADD.join(patientsGenderCrosstab,   patientsGenderCrosstab.SUBJECT_ID_GENDER == pIADD.SUBJECT_ID, 'inner')
pIADD = pIADD.join(patientsAdmitTypeCrossTab,   patientsAdmitTypeCrossTab.SUBJECT_ID_ADMISSION_TYPE == pIADD.SUBJECT_ID, 'inner')
pIADD = pIADD.join(patientsInsuranceTypeCrossTab,   patientsInsuranceTypeCrossTab.SUBJECT_ID_INSURANCE == pIADD.SUBJECT_ID, 'inner')
pIADD = pIADD.join(patientsADTdiagnosisCrossTab,   patientsADTdiagnosisCrossTab.SUBJECT_ID_DIAGNOSIS == pIADD.SUBJECT_ID, 'inner')

# COMMAND ----------

print (pIADD.columns)

# COMMAND ----------


# let's build model for entire patients (not just Sepsis on Admit) so use pIADD not PSITD
feature_cols = pIADD.columns

# remove categorical columns that have been replaced with 1-hot crosstab
exclude_feature_cols = ['SUBJECT_ID', 'GENDER', 'TARGET', 'HADM_ID', 'ADMITTIME',  \
                        'ADMISSION_TYPE' ,'INSURANCE', 'DIAGNOSIS',  'SUBJECT_ID_XCODE', \
                        'SUBJECT_ID_GENDER',   'SUBJECT_ID_ADMISSION_TYPE',   'SUBJECT_ID_INSURANCE', 'SUBJECT_ID_DIAGNOSIS' ]

for x in exclude_feature_cols:
#  print (x)
  feature_cols.remove(x)

vect_assembler = VectorAssembler(inputCols = feature_cols, outputCol="features")

# abbreviate the dataframe name and exclude cols

pIADD = pIADD.select([column  for column in pIADD.columns if column not in exclude_feature_cols])

final_data = vect_assembler.transform(pIADD)


# COMMAND ----------

final_data.printSchema()

# COMMAND ----------

final_data.select('features').show(5)

# COMMAND ----------

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scalerModel = scaler.fit (final_data)
cluster_final_data = scalerModel.transform(final_data)

# COMMAND ----------

kmeans3 = KMeans(featuresCol="scaledFeatures", k=3)
kmeans2 = KMeans(featuresCol="scaledFeatures", k=2)

model_k3 = kmeans3.fit(cluster_final_data)
model_k2 = kmeans2.fit(cluster_final_data)

wssse_k3=model_k3.computeCost(cluster_final_data)
wssse_k2=model_k2.computeCost(cluster_final_data)

# COMMAND ----------

print("with k=3: "+ str(wssse_k3) + ",  with k=2: " + str(wssse_k2))

# COMMAND ----------

for k in range(2,9):
	kmeans = KMeans(featuresCol='scaledFeatures', k=k)
model = kmeans.fit(cluster_final_data)
wssse = model.computeCost(cluster_final_data)
print("with K={}" , format(k))
print ("wtihin Set Sum of squared errors = " + str(wssse))
print "--"  *30


# COMMAND ----------



final_data=final_data.select('features', 'LABEL')    # LABEL is yes / no for mortality likelihood in hospital 
train, test = final_data.randomSplit( [ 0.7, 0.3] )
model = LogisticRegression(labelCol='LABEL')
model = model.fit(train)
summary = model.summary
summary.predictions.describe()

# COMMAND ----------


from pyspark.ml.evaluation import BinaryClassificationEvaluator

predictions=model.evaluate(test)
predictions.predictions.show()
evaluator=BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='LABEL')
evaluator.evaluate(predictions.predictions)
#       …. output is 0.7758


model1=LogisticRegression(labelCol='LABEL')
model1= model1.fit(final_data)



# COMMAND ----------
############################# COMMON CODE CDSW DataBricks September 10  2018 5:38 #######################################################
#########################################################################################################################################

"""
test_data=assembler.transform(test)
results = model1.transform(test_data)


results.show()
results.select('LABEL', 'prediction').show()
# shows mortality prediction  with prediction = 0.0 or 1.0 
"""
