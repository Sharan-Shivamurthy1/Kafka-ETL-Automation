BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles"
. $BASE_FOLDER/inbound_environment.sh

currentdate=`date +%y/%m/%d-%H:%M:%S`
echo currentdate $currentdate

SEQ_NUM=$1

#Getting the interface_details from the TESTING_CONFIG_DIM_FACTS table
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT SEQ_NO,DIMENTION_FACTS,NVL(INTERFACE_ID,0),TABLE_TYPE,JOB_NAME,SOURCE_TABLE,TARGET_TABLE,STAGE FROM TESTING_CONFIG_DIM_FACTS WHERE SEQ_NO=$SEQ_NUM; 
exit;
EOF`

DIMENTION_FACTS=$(echo $var | awk -F '[ ]' '{print $2}')
INTERFACE_ID=$(echo $var | awk -F '[ ]' '{print $3}')
TABLE_TYPE=$(echo $var | awk -F '[ ]' '{print $4}')
JOB_NAME=$(echo $var | awk -F '[ ]' '{print $5}')
SOURCE_TABLE=$(echo $var | awk -F '[ ]' '{print $6}')
TARGET_TABLE=$(echo $var | awk -F '[ ]' '{print $7}')
STAGE=$(echo $var | awk -F '[ ]' '{print $8}')
echo DIMENTION_FACTS: $DIMENTION_FACTS
echo INTERFACE_ID: $INTERFACE_ID
echo TABLE_TYPE: $TABLE_TYPE
echo STAGE: $STAGE
SOURCE_TABLE=$(echo $SOURCE_TABLE | sed -e 's/[\r\n]//g')

var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_NAME,SOURCE_TABLE FROM TESTING_CONFIG_DIM_FACTS WHERE SEQ_NO=$SEQ_NUM AND STAGE='DWDD'; 
exit;
EOF`
JOBNAME=$(echo $var | awk -F '[ ]' '{print $1}')
SOURCE_TABLE=$(echo $var | awk -F '[ ]' '{print $2}')
echo JOB_NAME: $JOBNAME
echo SOURCE_TABLE: $SOURCE_TABLE
JOBNAME=$(echo $JOBNAME | sed -e 's/[\r\n]//g')  #trimming the new line characters
#End of Getting JOB_NAME from the TESTING_JOBS table
TARGET_TABLE=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT TARGET_TABLE FROM TESTING_CONFIG_DIM_FACTS WHERE SEQ_NO=$SEQ_NUM AND STAGE='DWDD'; 
exit;
EOF`
echo TARGET_TABLE is $TARGET_TABLE
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
SELECT MAX(MAX_SRC_JOB_RUN_ID) FROM JOB_PROCESS_CONTROL_SRC WHERE JOB_RUN_ID=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`

MAX_SRC_JOB_RUN_ID=$(echo $MAX_SRC_JOB_RUN_ID | sed -e 's/[\r\n]//g')
echo MAXIMUM SOURCE JOB_RUN_ID: $MAX_SRC_JOB_RUN_ID
#End of Getting the MAXIMUM SOURCE_JOB_RUN_ID

x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target

source=$(echo $source | sed -e "s/,/%' OR OBJECT_NAME LIKE '%/g")
echo onject name is $source 

SOURCEOBJ=%$source%
echo $SOURCEOBJ


#SELECT NVL(MAX(JOB_RUN_ID),0), NVL(MIN(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>$MAX_SRC_JOB_RUN_ID AND TARGET_OBJECT_ID IN (SELECT OBJECT_ID FROM JOB_OBJECT_MASTER WHERE OBJECT_NAME IN ($SOURCE_TABLE) AND OBJECT_LAYER='DW_3NF'GROUP BY OBJECT_Id) AND JOB_RUN_STATUS=1 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');

#Getting MAXIMUM and MINIMUM SOURCE JOB_RUN_ID for current load
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0), NVL(MIN(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>$MAX_SRC_JOB_RUN_ID AND TARGET_OBJECT_ID IN (SELECT OBJECT_ID FROM JOB_OBJECT_MASTER WHERE OBJECT_NAME IN (SELECT OBJECT_NAME FROM JOB_OBJECT_MASTER WHERE OBJECT_NAME LIKE '%$SOURCEOBJ%') AND OBJECT_LAYER='DW_3NF' GROUP BY OBJECT_ID) AND JOB_RUN_STATUS=1 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
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
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','RECORDS ARE NOT AVAILABLE IN ${source} TO LOAD INTO ${target}','NO RECORDS AVAILABLE TO PROCESS','FAILED',SYSTIMESTAMP,'No Data Available to process');
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

#JOB_AVAILABILITY in wrappers
if [ $wrapper_directory/$JOBNAME.sh ]  #Checking for job availability
then
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
echo JOB PRESENT IN THE WRAPPERS
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','${JOBNAME} IS PRESENT IN WRAPPERS','JOB_AVAILABILITY','PASSED',SYSTIMESTAMP,'JOB IS PRESENT IN WRAPPERS');
exit;
EOF
#Executing the job
echo Job Starts................
sh $wrapper_directory/$JOBNAME.sh


#Getting the latest job_run_id
job_run_id=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME ='$JOBNAME'); 
exit;
EOF`

job_run_id=$(echo $job_run_id | sed -e 's/[\r\n]//g')
echo job_run_id $job_run_id
#End of Getting the latest job_run_id

#####ERROR LOG CHECK #####

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


#validating the data between source and target
SOURCE_DATA=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
$source_query minus $target_query;
exit;
EOF`
echo data_valid $SOURCE_DATA
TARGET_DATA=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
$target_query minus $source_query;
exit;
EOF`
echo data_valid $TARGET_DATA

if [ "$SOURCE_DATA" == "$TARGET_DATA" ]
then
echo DATA MATCHING
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','SOURCE TABLE ${source} DATA IS MATCHING WITH TARGET TABLE ${target}','DATA_VALIDATION','PASSED',SYSTIMESTAMP,'SOURCE DATA IS MATCHING WITH TARGET DATA');
exit;
EOF
else
echo DATA NOT MATCHING
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','SOURCE TABLE ${source} DATA IS NOT MATCHING WITH TARGET TABLE ${target}','DATA_VALIDATION','FAILED',SYSTIMESTAMP,'SOURCE DATA IS NOT MATCHING WITH TARGET DATA');
exit;
EOF
fi

#count check between source and target
#SOURCE_TARGET=`sqlplus -s $DWDDDB_CONNECTION <<EOF
#set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
#SELECT COUNT(*) FROM ($target_query) MINUS SELECT COUNT(*) FROM ($source_query);
#exit;
#EOF`
#echo count is $SOURCE_TARGET
#TARGET_SOURCE=`sqlplus -s $DWDDDB_CONNECTION <<EOF
#set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
#SELECT COUNT(*) FROM ($source_query) MINUS SELECT COUNT(*) FROM ($target_query);
#exit;
#EOF`
#echo count is $TARGET_SOURCE


#COUNT
SOURCE_TARGET=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
SELECT NVL(COUNT(*),0) FROM($target_query MINUS $source_query);
exit;
EOF`
echo count is $SOURCE_TARGET
TARGET_SOURCE=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off feedback off trimspool off echo off newpage none pagesize 0 linesize 10000 colsep , 
SELECT NVL(COUNT(*),0) FROM($source_query MINUS $target_query);
exit;
EOF`
echo count is $TARGET_SOURCE
#COUNT


if [ "$SOURCE_TARGET" == "$TARGET_SOURCE" ]
then
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
echo COUNT MATCHING
echo SOURCE AND TARGET COUNT IS MATCHING
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','SOURCE ${source} AND TARGET ${target} COUNT VALIDATION','COUNT_VALIDATION','PASSED',SYSTIMESTAMP,'SOURCE ${source} AND TARGET ${target} COUNT MATCHING');
exit;
EOF
else  #Count Validation
echo SOURCE AND TARGET COUNT IS NOT MATCHING
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','SOURCE ${source} AND TARGET ${target} COUNT VALIDATION','COUNT_VALIDATION','FAILED',SYSTIMESTAMP,'SOURCE ${source} AND TARGET ${target} COUNT NOT MATCHING');
exit;
EOF

fi

################################### Code to check Null values ########################################
Null_columns=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off
select column_name from all_tab_columns where table_name = '$target' and nullable = 'N'; 
exit;
EOF`

null_col_val=''

for null_col in ${Null_columns[@]}
do
	#echo ========Columns====$null_col
	null_col_val=$null_col_val,$null_col
	#echo $nulll_col_val
done
#echo $nulll_col_val
#exit
null_col_val=`echo $null_col_val|sed 's/[0-9]//g'`
null_col_val=`echo $null_col_val|sed 's/rows,selected//g'`
null_col_val=`echo $null_col_val|sed 's/,,.$//g'`
null_col_val=`echo $null_col_val|sed 's/^,//g'`
echo Not null columns after substitution: $null_col_val

null_cols=`echo $null_col_val|sed 's/,/ is null or /g'`
null_cols="$null_cols is null"

############# code to count null records ###############
#echo $null_cols
null_query="select count(*) from $target where $null_cols"

#echo $null_query

Null_value_cnt=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off
$null_query;
exit;
EOF`

#Trim
Null_value_cnt=`echo $Null_value_cnt|sed 's/^ //'`

#echo ===$Null_value_cnt===
#INTERFACEID=1
#INTERFACENAME="Dimension"
#SUBTYPE="NA"
#SOURCENAME="3NF"

if [ $Null_value_cnt -eq 0 ]; then
	echo Null record not present

sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','NULL records are not prsent in ${target}','NO null records','PASSED',SYSTIMESTAMP,'No NULL RECORDS');
exit;
EOF
	
else
	echo $Null_value_cnt records contain null values
	############## code to fetch null records #######
	null_record_fetch_query="select $null_col_val from $target where $null_cols"
	Null_value_records=`sqlplus -s $DWDDDB_CONNECTION <<EOF
	set head on
	set COLSEP,
	set linesize 32767
	SET TRIMSPOOL ON
	SET TRIMOUT ON
	$null_record_fetch_query;
	exit;
	EOF`

	echo Null records:$Null_value_records

sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','NULL records are  present in ${target}','null records','FAILED',SYSTIMESTAMP,'NULL RECORDS');
exit;
EOF

fi
################################# End of Null value check ################################################

##################### Code to check duplicate values #######################
all_columns=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off
select column_name from all_tab_columns where table_name = '$target';
exit;
EOF`

all_col_val=''

for all_col in ${all_columns[@]}
do
        #echo ========Columns====$null_col
        all_col_val=$all_col_val,$all_col
        #echo $nulll_col_val
done
#echo $all_col_val
#exit
all_col_val=`echo $all_col_val|sed 's/[0-9]//g'`
all_col_val=`echo $all_col_val|sed 's/rows,selected//g'`
all_col_val=`echo $all_col_val|sed 's/,,.$//g'`
all_col_val=`echo $all_col_val|sed 's/^,//g'`

#############code to fetch dplicate records ##############
duplicate_check_query="select $all_col_val,count(*) from $target group  by $all_col_val having count(*) > 1"

#echo $duplicate_check_query
#exit
duplicate_check_result=`sqlplus -s $DWDDDB_CONNECTION <<EOF
set head off
$duplicate_check_query;
exit;
EOF`
#echo Duplicate result:$duplicate_check_result
#### Trim ######
duplicate_check_result=`echo $duplicate_check_result|sed 's/^ | $//'`
#echo =====$duplicate_check_result=======
if [[ "$duplicate_check_result" =~ 'no rows selected' ]];then
	echo No duplicate records found

	sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','Duplicate records are not prsent in ${target}','NO duplicate records','PASSED',SYSTIMESTAMP,'No DUPLICATE RECORDS');
exit;
EOF

else
	echo Duplicate records:$duplicate_check_result
	
	sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','Duplicate records are  present in ${target}','Duplicate records','FAILED',SYSTIMESTAMP,'DUPICATE RECORDS');
exit;
EOF
	
fi
###################################### End of Duplicate check ###########################################
#JOB_PROCESS_CONTROL Check
job_run_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_RUN_STATUS FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$job_run_id; 
exit;
EOF`
echo job_run_status is $job_run_status
if [ $job_run_status = 1 ]
then
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
echo JOB SUCCESSFUL
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','JOB_EXECUTION FOR ${DIMENTION_FACTS}','JOB_RUN_STATUS IS ${job_run_status}','PASSED',SYSTIMESTAMP,'JOB IS SUCCESSFULL WITHOUT ANY ERRORS');
exit;
EOF

else
echo JOB FAILED
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS)
values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','JOB_EXECUTION FOR ${DIMENTION_FACTS}','JOB_RUN_STATUS IS ${job_run_status}','FAILED',SYSTIMESTAMP,'JOB IS FAILED');
exit;
EOF

fi
#JOB_PROCESS_CONTROL Check


else  #Checking for job availability
echo JOB IS NOT AVAILABLE IN THE WRAPPERS
x="'"
source=$(echo $SOURCE_TABLE | sed -e "s/'//g")
echo source: $source
target=$(echo $TARGET_TABLE | sed -e "s/'//g")
echo target: $target
sqlplus -s $DB_CONNECTION <<EOF
insert into DIM_FACTS_TEST_LOG(INTERFACE_ID,DIMENTION_FACTS,SOURCE_TABLE,TARGET_TABLE,TABLE_TYPE,STAGE,TEST_CASE,STEP_NAME,TEST_RESULT,EXECUTE_DATE,COMMENTS) values('$INTERFACE_ID','$DIMENTION_FACTS','$source','$target','$TABLE_TYPE','$STAGE','${JOBNAME} IS NOT PRESENT IN WRAPPERS','JOB_AVAILABILITY','FAILED',SYSTIMESTAMP,'JOB IS NOT AVAILBLE');
exit;
EOF

fi  #Checking for job availability
#End of JOB_AVAILABILITY in wrappers

fi  #Record availability to process