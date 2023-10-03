BASE_FOLDER="/home/sshuser/Test/Test_vharm"
. $BASE_FOLDER/inbound_environment.sh

currentdate=`date +%y/%m/%d-%H:%M:%S`
echo currentdate $currentdate

SEQ_NUM=$1

#Getting the interface_details from the TESTING_CONFIG table
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT INTERFACE_ID, INTERFACE_NAME, INTERFACE_SUBTYPE, SOURCE_NAME FROM TESTING_CONFIG WHERE SEQ_NO=$SEQ_NUM; 
exit;
EOF`

INTERFACEID=$(echo $var | awk -F '[ ]' '{print $1}')
INTERFACENAME=$(echo $var | awk -F '[ ]' '{print $2}')
SOURCENAME=$(echo $var | awk -F '[ ]' '{print $3}')

echo INTERFACE_ID: $INTERFACEID
echo INTERFACE_NAME: $INTERFACENAME
echo SOURCE_NAME: $SOURCENAME
#End of Getting the interface_details from the TESTING_CONFIG table

#Getting JOB_NAME from the TESTING_JOBS table
SOURCE_TABLE=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT TABLE_NAME FROM TESTING_JOBS WHERE SEQ_NO=$SEQ_NUM AND STAGE='DWDD'; 
exit;
EOF`
SOURCE_TABLE=$(echo $3NF_TABLE | sed -e 's/[\r\n]//g')
echo SOURCE_TABLE is $SOURCE_TABLE
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_NAME, TABLE_NAME FROM TESTING_JOBS WHERE SEQ_NO=$SEQ_NUM AND STAGE='DWDD'; 
exit;
EOF`
JOBNAME=$(echo $var | awk -F '[ ]' '{print $1}')
SOURCE_TABLE=$(echo $var | awk -F '[ ]' '{print $2}')
echo JOB_NAME: $JOBNAME
echo 3NF_TABLE: $SOURCE_TABLE
JOBNAME=$(echo $JOBNAME | sed -e 's/[\r\n]//g')  #trimming the new line characters
#End of Getting JOB_NAME from the TESTING_JOBS table


#Getting Previous Successful JOB_RUN_ID
PRE_SUCCESS_JOB_RUN_ID=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
exit;
EOF`

PRE_SUCCESS_JOB_RUN_ID=$(echo $PRE_SUCCESS_JOB_RUN_ID | sed -e 's/[\r\n]//g')
echo PREVIOUS SUCCESSFUL JOB_RUN_ID: $PRE_SUCCESS_JOB_RUN_ID
#End of Getting Previous Successful JOB_RUN_ID


#Getting the MAXIMUM SOURCE_JOB_RUN_ID
MAX_SRC_JOB_RUN_ID=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX_SRC_JOB_RUN_ID,0) FROM JOB_PROCESS_CONTROL_SRC WHERE JOB_RUN_ID=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`

MAX_SRC_JOB_RUN_ID=$(echo $MAX_SRC_JOB_RUN_ID | sed -e 's/[\r\n]//g')
echo MAXIMUM SOURCE JOB_RUN_ID: $MAX_SRC_JOB_RUN_ID
#End of Getting the MAXIMUM SOURCE_JOB_RUN_ID


#Getting MAXIMUM and MINIMUM SOURCE JOB_RUN_ID for current load
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0), NVL(MIN(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>$MAX_SRC_JOB_RUN_ID AND TARGET_OBJECT_ID IN (SELECT LISTAGG(OBJECT_ID,',') WITHIN GROUP (ORDER BY OBJECT_ID) FROM JOB_OBJECT_MASTER WHERE OBJECT_NAME IN ($SOURCE_TABLE) AND OBJECT_LAYER='DW_3NF') AND JOB_RUN_STATUS=1 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
exit;
EOF`

MAX_JOB_RUN_ID_CURRENT=$(echo $var | awk -F '[ ]' '{print $1}')
MIN_JOB_RUN_ID_CURRENT=$(echo $var | awk -F '[ ]' '{print $2}')

MAX_JOB_RUN_ID_CURRENT=$(echo $MAX_JOB_RUN_ID_CURRENT | sed -e 's/[\r\n]//g')
echo MAXIMUM SOURCE JOB_RUN_ID CURRENT LOAD: $MAX_JOB_RUN_ID_CURRENT

MIN_JOB_RUN_ID_CURRENT=$(echo $MIN_JOB_RUN_ID_CURRENT | sed -e 's/[\r\n]//g')
echo MINIMUM SOURCE JOB_RUN_ID CURRENT LOAD: $MIN_JOB_RUN_ID_CURRENT
#End of Getting MAXIMUM and MINIMUM SOURCE JOB_RUN_ID for current load
if [ $MAX_JOB_RUN_ID_CURRENT -eq 0 ] && [ $MIN_JOB_RUN_ID_CURRENT -eq 0 ]  #Record availability to process
then
echo NO RECORDS AVAILABLE TO PROCESS
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type) values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','DATA_VALIDATION','JOB_EXECUTION','FAILED','No Data Available to process',SYSTIMESTAMP,'DIM');
exit;
EOF

else  #Record availability to process
echo RECORDS AVAILABLE TO PROCESS

#Getting the source query
source_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(SOURCE_QUERY, dbms_lob.getlength(SOURCE_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

echo SOURCE_QUERY: $source_query
#End of getting the source query


#Replacing with the actual data
source_query=$(echo $source_query | sed -e "s/\$MIN_RUN_ID_CURRENT/${MIN_JOB_RUN_ID_CURRENT}/g")
source_query=$(echo $source_query | sed -e "s/\$MAX_RUN_ID_CURRENT/${MAX_JOB_RUN_ID_CURRENT}/g")
echo SOURCE_QUERY AFTER REPLACEMENT: $source_query
#End of Replacing with the actual data
#source_query=$(echo $source_query | sed -e sed -e 's/","/ /g')

#Getting data for current load from 3nf table
source_data=`sqlplus -s $TNFDB_CONNECTION <<EOF
set head off feedback off trimspool off newpage none pagesize 0 linesize 10000 colsep , 
$source_query order by 1 asc;
exit;
EOF`
source_data=$(echo "$source_data" | sed -e 's/[\r\n]/,/g')
source_data=$(echo "$source_data" | sed -e 's/[\r\t]//g')
#source_data=$(echo "$source_data" | sed -e 's//,/g')
echo source_data: "$source_data"
#End of Getting data for current load from 3nf table


#JOB_AVAILABILITY in wrappers
if [ $wrapper_directory/$JOBNAME.sh ]  #Checking for job availability
then
echo JOB PRESENT IN THE WRAPPERS
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type) values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_AVAILABILITY','JOB_AVAILABILITY IN WRAPPERS','PASSED',SYSTIMESTAMP,'DIM');
exit;
EOF
#Executing the job
echo Job Starts................
#sh $wrapper_directory/$JOBNAME.sh


#Getting the latest job_run_id
job_run_id=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT MAX(JOB_RUN_ID) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME ='$JOBNAME'); 
exit;
EOF`

job_run_id=$(echo $job_run_id | sed -e 's/[\r\n]//g')
echo job_run_id $job_run_id
#End of Getting the latest job_run_id

#Getting the target query
target_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(TARGET_QUERY, dbms_lob.getlength(TARGET_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

echo TARGET_QUERY: $target_query
#End of Getting the target query


#Replacing with the actual values
target_query=$(echo $target_query | sed -e "s/\$job_run_id/${job_run_id}/g")

echo TARGET_QUERY AFTER REPLACEMENT: "$target_query"
#End of Replacing with the actual values


#Getting the data from the staging table
DIM_DATA=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
$target_query order by 1 asc;
exit;
EOF`


#STAGING_DATA=$(echo "$STAGING_DATA" | sed -e 's/[\r\n]//g')
#DIM_DATA=$(echo "$DIM_DATA" | sed -e 's/[\r\n]/,/g')
DIM_DATA=$(echo "$DIM_DATA" | sed -e 's/[\r\t]//g')
echo DIM_DATA: "$DIM_DATA"
#End of getting data from target table

#Redirecting source and target data into csv files
echo "$source_data" > $log_file_path/source_data_$job_run_id.csv
echo "$DIM_DATA" > $log_file_path/target_data_$job_run_id.csv
#End of  redirecting source and target data into csv files


#Comparing source data and target data
dif=$(diff -r $log_file_path/source_data_$job_run_id.csv $log_file_path/target_data_$job_run_id.csv | wc -l)
echo DIFF: $dif
#Comparing source data and target data


#Get the record count in source and target
source_count=$(wc -l < $log_file_path/source_data_$job_run_id.csv)
target_count=$(wc -l < $log_file_path/target_data_$job_run_id.csv)
echo SOURCE_COUNT: $source_count
echo TARGET_COUNT: $target_count

#Comparing data between source and target
echo COMPARISON

if [ $source_count -eq $target_count ]  #Count Validation
then
echo SOURCE AND TARGET COUNT IS MATCHING
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','COUNT_VALIDATION','SOURCE AND TARGET COUNT VALIDATION','PASSED',SYSTIMESTAMP,'dim');
exit;
EOF

if [ $dif -eq 0 ]  #Data Validation
then
echo DATA MATCHING
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','DATA_VALIDATION','SOURCE AND TARGET DATA VALIDATION','PASSED',SYSTIMESTAMP,'dim');
exit;
EOF

else  #Data Validation
echo DATA NOT MATCHING
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','DATA_VALIDATION','SOURCE AND TARGET DATA VALIDATION','FAILED','Data from the 3NF table is not matching with the TARGET data',SYSTIMESTAMP,'dim');
exit;
EOF

fi  #Data Validation

else  #Count Validation
echo SOURCE AND TARGET COUNT IS NOT MATCHING

sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','3nf to dim or fact','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','COUNT_VALIDATION','SOURCE AND TARGET COUNT VALIDATION','FAILED','Data from the 3NF is not matching with the TARGET data',SYSTIMESTAMP,'dim');
exit;
EOF

fi  #Count Validation
#End of Comparing data between source and target


#Duplicate check in target
duplicate_query=$(echo $target_query | awk -F ' WHERE' '{print $1}')
echo DUPLICATE: $duplicate_query

DUPLICATE_DATA=`sqlplus -s $STGDB_CONNECTION <<EOF
set head off feedback off echo off newpage none pagesize 0 linesize 10000 colsep , 
$duplicate_query;
exit;
EOF`


DUPLICATE_DATA=$(echo "$DUPLICATE_DATA" | sed -e 's/[\r\t]//g')
DUPLICATE_DATA=$(echo "$DUPLICATE_DATA" | sed -e 's/ //g')

echo "$DUPLICATE_DATA" > $log_file_path/duplicate_data_$job_run_id.csv

duplicate_count=$(echo $(sort $log_file_path/duplicate_data_$job_run_id.csv | uniq -d))
duplicate_count=$(echo "$duplicate_count" | sed -e 's/[\r\n]//g')

echo DUPLICATE_COUNT: $duplicate_count

if [ "$duplicate_count" == "" ]
then
echo NO DUPLICATES
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','3nf to dim or fact','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','DATA_VALIDATION','DUPLICATE CHECK','PASSED',SYSTIMESTAMP,'dim_fact');
exit;
EOF

else
echo DUPLICATE PRESENT
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','3nf to dim or fact','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','DATA_VALIDATION','DUPLICATE CHECK','FAILED','Duplicate records present',SYSTIMESTAMP,'dim_fact');
exit;
EOF

fi

#End of Duplicate check in target


#JOB_PROCESS_CONTROL Check
job_run_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_RUN_STATUS FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$job_run_id; 
exit;
EOF`

if [ $job_run_status = 1 ]
then
echo JOB SUCCESSFUL
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_EXECUTION','JOB_RUN_STATUS','PASSED',SYSTIMESTAMP,'dim');
exit;
EOF

else
echo JOB FAILED
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_EXECUTION','JOB_RUN_STATUS','FAILED','JOB_RUN_STATUS is ${job_run_status}',SYSTIMESTAMP,'dim');
exit;
EOF

fi
#JOB_PROCESS_CONTROL Check


else  #Checking for job availability
echo JOB IS NOT AVAILABLE IN THE WRAPPERS
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','DWDD','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_AVAILABILITY','JOB_AVAILABILITY IN WRAPPER','FAILED','Job is not present in the wrappers directory',SYSTIMESTAMP,'dim');
exit;
EOF

fi  #Checking for job availability
#End of JOB_AVAILABILITY in wrappers

fi  #Record availability to process