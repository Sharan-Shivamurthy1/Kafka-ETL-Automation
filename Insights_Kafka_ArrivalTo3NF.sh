BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles"
BASE_FOLDER_FUNCTIONLIBRARY="/home/sshuser/Test_Automation/FunctionLibrary"
. $BASE_FOLDER/KafkaEnvironmentVariables.sh
. $BASE_FOLDER_FUNCTIONLIBRARY/Functions.sh
. $BASE_FOLDER_FUNCTIONLIBRARY/kafka_conform_Staging.sh
BASE_FOLDER_SCRIPTS="/home/sshuser/Test_Automation/Scripts"
. $BASE_FOLDER/inbound_environment.sh
. $BASE_FOLDER_FUNCTIONLIBRARY/kafka_Stagingto3NF.sh

#SEQ_NUM='53'
#TOPICNAME='customer_contract'
#FLAG_PROCESS=0
#HIVE_TABLE='Agreement'

#SEQ_NUM='54'
#TOPICNAME='billing_account'
#FLAG_PROCESS=0
#HIVE_TABLE='billing_account'

SEQ_NUM=$1
TOPICNAME=$2
FLAG_PROCESS=0
HIVE_TABLE=$3

v_date=`date +%Y%m%d`
#Getting the interface_details from the TESTING_CONFIG table
echo "INTERFACE DETAILS"
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT INTERFACE_ID, INTERFACE_NAME, INTERFACE_SUBTYPE FROM TESTING_CONFIG WHERE SEQ_NO=$SEQ_NUM; 
exit;
EOF`

INTERFACEID=$(echo $var | awk -F '[ ]' '{print $1}')
INTERFACENAME=$(echo $var | awk -F '[ ]' '{print $2}')
SUBTYPE=$(echo $var | awk -F '[ ]' '{print $3}')

echo INTERFACE_ID: $INTERFACEID
echo INTERFACE_NAME: $INTERFACENAME
echo INTERFACE_SUBTYPE: $SUBTYPE
#End of Getting the interface_details from the TESTING_CONFIG table

#Get the control table data
var=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT PROCESS_DIR_PATH, CONFORMED_DIR_PATH, COMPLETE_DIR_PATH, ERROR_DIR_PATH, REJECT_DIR_PATH FROM INTERFACE_FILE_MASTER WHERE INTERFACE_ID='$INTERFACEID' AND INTERFACE_NAME='$INTERFACENAME' AND INTERFACE_SUB_TYPE='$SUBTYPE'; 
exit;
EOF`

procdirpath=$(echo $var | awk -F '[ ]' '{print $1}')
cnfldirpath=$(echo $var | awk -F '[ ]' '{print $2}')
completedirpath=$(echo $var | awk -F '[ ]' '{print $3}')
errdirpath=$(echo $var | awk -F '[ ]' '{print $4}')
rejdirpath=$(echo $var | awk -F '[ ]' '{print $5}')

echo ProcessDir: $procdirpath
echo ComformedDir: $cnfldirpath
echo CompleteDir: $completedirpath
echo ErrorDir: $errdirpath
echo RejectDir: $rejdirpath
#End of Get the control table data

v_path=$adlpath$procdirpath
Expectedstatus=1
TESTCASE='Kafka validation'
echo "v_path=$v_path"

#Get Number of partition
GetNoOfPartition $TOPICNAME
v_nm_of_parti=$var

echo "ARRIVAL TO PROCESS"

#Getting JOB_NAME from the TESTING_JOBS table
GetJobName $SEQ_NUM 'PROCESS'
stringarray=($var)
for (( PartitionCounter=0; PartitionCounter<$v_nm_of_parti; PartitionCounter=PartitionCounter+1 ))
do
JobArray[PartitionCounter]=${stringarray[PartitionCounter]}".sh"
done
#End of Getting JOB_NAME from the TESTING_JOBS table

FinalCount=0
act_file_data=""
for (( PartitionCounter=0; PartitionCounter<$v_nm_of_parti; PartitionCounter=PartitionCounter+1 ))
do
#query to select job_id from job_master table
SOURCENAME=${stringarray[PartitionCounter]}
echo "JOB NAME=$SOURCENAME"
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select job_id,job_name,job_type from job_master where job_name = '$SOURCENAME';  
exit;
EOF
)

#storing data retreived from job_master table to local variables
v_job_id=$(echo $variable | awk -F '[ ]' '{print $1}')
v_job_name=$(echo $variable | awk -F '[ ]' '{print $2}')
v_job_type=$(echo $variable | awk -F '[ ]' '{print $3}')
echo v_job_id $v_job_id

#query to select job_run_id from JOB_PROCESS_CONTROL table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select job_run_id,job_id, job_end_date_time from (
select job_run_id,job_id, job_end_date_time,ROW_NUMBER() OVER (PARTITION by job_id ORDER BY JOB_END_DATE_TIME DESC) as rank from JOB_PROCESS_CONTROL where job_end_date_time is not null and job_id = $v_job_id)
where rank=1; 
exit;
EOF
)

#storing job_run_id retreived from JOB_PROCESS_CONTROL table to local variables
v_prev_job_run_id=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_prev_job_run_id $v_prev_job_run_id

#query to take last offset from the count from kafka_event table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select (MAX(to_number(UNTIL_OFFSET))) from kafka_event where TOPIC_NAME='$TOPICNAME' AND JOB_RUN_ID='$v_prev_job_run_id'; 
exit;
EOF
)
prev_untilOffset=$(echo $variable | awk -F '[ ]' '{print $1}')
echo "prev_untilOffset $prev_untilOffset"


#Run the job

JOB=${JobArray[PartitionCounter]}
echo "JOB STARTS...."
echo "" > nohup.out
nohup sh /insights/app/scripts/wrappers/$JOB
echo "JOB ENDS...."
# Get Start and End Time for Batch Duration
#Start Time
var=`sed -n '/StreamingContext: StreamingContext started/p' /home/sshuser/Test_Automation/Scripts/nohup.out`
StartTime=$(echo $var | awk -F '[ ]' '{print $2}')
StartTimeHr=$(echo $StartTime | awk -F ':' '{print $1}')
StartTimeMin=$(echo $StartTime | awk -F ':' '{print $2}')
StartTimeSec=$(echo $StartTime | awk -F ':' '{print $3}')
echo "StartTimeHr=$StartTimeHr, StartTimeMin=$StartTimeMin, StartTimeSec=$StartTimeSec"
StartTime=(`expr $StartTimeHr \* 3600 + $StartTimeMin \* 60 + $StartTimeSec`)

#End Time
var=`sed -n '/ReceiverTracker: ReceiverTracker stopped/p' /home/sshuser/Test_Automation/Scripts/nohup.out`
EndTime=$(echo $var | awk -F '[ ]' '{print $2}')
EndTimeHr=$(echo $EndTime | awk -F ':' '{print $1}')
EndTimeMin=$(echo $EndTime | awk -F ':' '{print $2}')
EndTimeSec=$(echo $EndTime | awk -F ':' '{print $3}')
echo "EndTimeHr=$EndTimeHr, EndTimeMin=$EndTimeMin, EndTimeSec=$EndTimeSec"
EndTime=(`expr $EndTimeHr \* 3600 + $EndTimeMin \* 60 + $EndTimeSec`)

TimeDiff=(`expr $EndTime - $StartTime`)
echo "TIME DIFFERENCE is $TimeDiff"

#Get Batch Duration from Db for particular Topic
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select BATCH_DURATION from EVENT_CONTROL_CONFIG where TOPIC_NAME = '$TOPICNAME';
exit;
EOF
)

#storing Batch Duration retreived from EVENT_CONTROL_CONFIG table to local variables
v_topic_batch_duration=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_topic_batch_duration $v_topic_batch_duration

#Validate if untill offset for prev job run is same as from offset for current job run
TESTCASE='validate batch duration'
Expectedstatus='Batch duration for job should be same as specified'
if [ "$v_topic_batch_duration" = "$TimeDiff" ]
then
echo "JOB RAN FOR EXPECTED BATCH DURSTION"
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The batch duration is as expected',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The batch duration is not as expected',sysdate); 
exit;
EOF
fi

#get New Job Run ID
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select job_run_id,job_id, job_end_date_time from (
select job_run_id,job_id, job_end_date_time,ROW_NUMBER() OVER (PARTITION by job_id ORDER BY JOB_END_DATE_TIME DESC) as rank from JOB_PROCESS_CONTROL where job_end_date_time is not null and job_id = $v_job_id)
where rank=1; 
exit;
EOF
)

#storing job_run_id retreived from JOB_PROCESS_CONTROL table to local variables
v_job_run_id=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_job_run_id $v_job_run_id

#query to validate if Job run is successful
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select JOB_RUN_STATUS from JOB_PROCESS_CONTROL where job_run_id = $v_job_run_id;
exit;
EOF
)

#storing data retreived from file_control table to local variables
v_job_run_status=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_job_run_status $v_job_run_status

if [ "$v_job_run_status" = "1" ]  #JOB_PROCESS_CONTROL check
then
echo "JOB EXECUTED SUCCESSFULLY FOR ARRIVAL"
else
echo JOB EXECUTION FAILED
fi

#query to take the from Offset from kafka_event table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select (MAX(to_number(from_offset))) from kafka_event where topic_name ='$TOPICNAME' AND JOB_RUN_ID='$v_job_run_id';
exit;
EOF
)

#storing the count retreived from kafka_event table to local variables
new_from_offset=$(echo $variable | awk -F '[ ]' '{print $1}')
echo new_from_offset $new_from_offset

#Validate if untill offset for prev job run is same as from offset for current job run
TESTCASE='validate offset starts from previous end offset'
Expectedstatus='Prev until offset equal to current from offset'

#query to take the count from kafka_event table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select sum(UNTIL_OFFSET - from_offset) from kafka_event where topic_name ='$TOPICNAME' and job_run_id = $v_job_run_id; 
exit;
EOF
)

#storing the count retreived from kafka_event table to local variables
v_tbl_count=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_tbl_count $v_tbl_count
FinalCount=(`expr $FinalCount + $v_tbl_count`)
echo "Final count: $FinalCount"

#validate File processing status and filename
TESTCASE='validate job run status'
Expectedstatus='Job run status should be 1'
ExpFileStatus=1
if [ "$v_job_run_status" = "$ExpFileStatus" ]
then
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The job run status is $v_job_run_status',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The job run status is not $v_job_run_status',sysdate); 
exit;
EOF
fi

#query to validate if file is created in file control table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select FILE_PROCESSING_STATUS,FILE_NAME from FILE_CONTROL where job_run_id = $v_job_run_id;
exit;
EOF
)

#storing data retreived from file_control table to local variables
v_file_processing_status=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_file_processing_status $v_file_processing_status
v_file_name=$(echo $variable | awk -F '[ ]' '{print $2}')
echo v_file_name $v_file_name

#validate File processing status and filename
TESTCASE='validate file status'
if [ "$v_tbl_count" = "0" ]
then
Expectedstatus='File should not be created'
ExpFileStatus=0
else
Expectedstatus='File status should be 1'
ExpFileStatus=1
fi
if [ "$v_file_processing_status" = "$ExpFileStatus" ]
then
echo "FILE PROCESSING STATUS IS CORRECT"
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The file status is $v_file_processing_status',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The file status is not $v_file_processing_status',sysdate); 
exit;
EOF
fi

TESTCASE='validate file name'
Expectedstatus='File name should be as expected'
if [ "$v_tbl_count" = "0" ]
then
FILE_NAME=''
else
FILE_NAME=$TOPICNAME'_'$PartitionCounter'_'$v_job_run_id
fi
if [ "$v_file_name" = "$FILE_NAME" ]
then
echo "FILE NAME IS DISPLAYED EXPECTED AND IS $v_file_name"
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The file name is as expected in file control and is $v_file_name',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The file name is not as expected in file control and is $v_file_name',sysdate); 
exit;
EOF
fi


Expectedstatus=1

#Check if the file-folders are present in the correct format depending upon the partitions using loop
if [ "$Expectedstatus" = 1 ]
then
	if [ $v_tbl_count -gt 0 ]
	then
 
   v_file_data_count=0

			DIR_NAME=$TOPICNAME'_'$PartitionCounter'_'$v_job_run_id      
      echo "DIR_NAME: $DIR_NAME"
      DIRNAME[PartitionCounter]=$DIR_NAME
      
      FileName=$(hadoop fs -ls $v_path'/'$DIR_NAME)
      echo "FileName: $FileName" 
      FILENAME[PartitionCounter]=$FileName
      
      FileCount=$(echo $FileName | awk -F $DIR_NAME '{print NF}')
      FileCount=(`expr $FileCount - 2`)
      echo "FileCount: $FileCount"
      
      FileName1=$(echo $FileName | awk -F $DIR_NAME '{print $3}')
      echo "FileName1: $FileName1"
      
			#if the file is present for a particular partition check the count and add it up to arrive the total count
			if [ 0 ]
			then
				v_file_counter=$(hadoop fs -cat $v_path'/'$DIR_NAME/part* | wc -l)
        v_file_data=$(hadoop fs -cat $v_path'/'$DIR_NAME/part*)
        echo "v_file_counter: $v_file_counter"
        echo "v_file_data: $v_file_data"
        act_file_data=$act_file_data$v_file_data
        echo "act_file_data: $act_file_data"
        
				v_file_data_count=(`expr $v_file_counter + $v_file_data_count`)
        echo "v_file_data_count: $v_file_data_count"
        FILECOUNT[PartitionCounter]=$v_file_data_count
        Act_FinalCount=(`expr $Act_FinalCount + $v_file_data_count`)
        echo "Act_FinalCount $Act_FinalCount"
			else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','INFO','The Directory $DIR_NAME is not present out of $v_nm_of_parti number of partitions',sysdate); 
exit;
EOF
			fi
echo v_file_data_count $v_file_data_count   
echo "v_file_data_count=$v_file_data_count"
echo "v_tbl_count=$v_tbl_count"
if [ "$v_file_data_count" = "$v_tbl_count" ]
then
echo "COUNT OF DATA READ BY FILE IS CORRECT"
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The count of data read is correct and is $v_file_data_count',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE) 
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The count does not match',sysdate); 
exit;
EOF
fi  #end of count validation


	fi
fi
done

echo "FinalCount $FinalCount"
echo "Act_FinalCount $Act_FinalCount"

Acount=$(echo $act_file_data | awk -F '},{' '{print NF}')
if [ "$Acount" = "0" ]
then
ExpConformCount=$Act_FinalCount
else
Acount=(`expr $Acount - 1`)
ExpConformCount=(`expr $Acount \* 2`)
fi
#echo "ExpConformCount : $ExpConformCount"

#Validate the merged count
if [ "$FinalCount" = "$Act_FinalCount" ]
then
echo "FINAL DATA COUNT READ BY ALL PARTITION IS CORRECT"
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The final count of data is $v_file_data_count',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE) 
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The final count does not match',sysdate); 
exit;
EOF
fi

echo "act_file_data=$act_file_data"

echo "PROCESS TO CONFORM"
#Get Job Name
GetJobName $SEQ_NUM 'CONFORM'
JOBNAME=$var
echo "Conform Jobname: $JOBNAME"
#Executing the job
echo "JOB STARTS: $JOBNAME"
echo "" > nohup.out
nohup sh $wrapper_directory/$JOBNAME.sh
echo "JOB RUN ENDS"

#Get Job Run ID
GetJobDetails $JOBNAME
v_job_id=$v_job_id
echo "v_job_id=$v_job_id"

#Get Job Run id
GetCurrentJobRunID $v_job_id
v_conform_job_run_id=$v_job_run_id

#query to validate if Job run is successful from Conform stage
GetJobRunStatus $v_conform_job_run_id
v_conform_job_run_status=$v_job_run_status
echo "Conform Job run status: $v_conform_job_run_status"

if [ "$v_conform_job_run_status" = "1" ]  #JOB_PROCESS_CONTROL check
then
echo "JOB EXECUTED SUCCESSFULLY FOR CONFORM LAYER"
else
echo JOB EXECUTION FAILED
fi



#Hive Validation

typeset -u colhead 
typeset -u line

var=$(hive -e "use governed_data; set hive.cli.print.header=true; select * from $HIVE_TABLE limit 0;")
var=${var//$HIVE_TABLE./}

colhead=$(echo $var | awk -F ' create_job_run_id' '{print $1}')
columnname=${colhead// /,}

#file_count=$process_count
#file_count=(`expr $file_count - 1`)
hive_count=$(hive -e "use governed_data; set hive.cli.print.header=false; set hive.compute.query.using.stats=false; select count(*) from $HIVE_TABLE where create_job_run_id = $v_conform_job_run_id;")

echo "HIVE COUNT: $hive_count"

#Count comparison between process file and hive table
if [[ $ExpConformCount -eq $hive_count ]]
then
echo HIVE COUNT IS MATCHING
fi
#End of Count comparison between process file and hive table

#Data comparison between process file and hive table
#hive_data=$(hive -e "use governed_data; set hive.cli.print.header=true; select $columnname from $HIVE_TABLE where create_job_run_id = $v_conform_job_run_id;")
columnname='*'
hive_data=$(hive -e "use governed_data; set hive.cli.print.header=false; select $columnname from $HIVE_TABLE where create_job_run_id = $v_conform_job_run_id;")
echo "HIVE DATA is $hive_data"

file_data=$act_file_data
echo "filedata is $file_data"
#file_data=$(echo "$file_data" | tr '","' ' ')
if [ "$hive_data" = "" ]
then
  echo "HIVE DATA NOT CREATED-FAIL"
else
#  if [ "$hive_data" = "$file_data" ]
#  then
#  echo HIVE DATA IS MATCHING
#  else
  echo "HIVE DATA IS CREATED"
#  fi
fi

for (( PartitionCounter=0; PartitionCounter<$v_nm_of_parti; PartitionCounter=PartitionCounter+1 ))
do
complete_data_count=$(hadoop fs -cat $adlpath$completedirpath'/'${DIRNAME[PartitionCounter]}'/part*' | wc -l)
echo "complete_data_count: $complete_data_count"
complete_data=$(hadoop fs -cat $adlpath$completedirpath'/'${DIRNAME[PartitionCounter]}'/part*')
if [ "${FILECOUNT[PartitionCounter]}" = "$complete_data_count" ]  #Data validation in complete directory
then
echo "DATA COUNT MATCHING for complete directory"
fi

if [ "$v_file_data" = "$complete_data" ]  #Data validation in complete directory
then
echo "DATA MATCHING for complete directory"
fi
done
#Data comparison between process file and hive table

#Validate Confomed directory for .avro file presence
#for (( PartitionCounter=0; PartitionCounter<$v_nm_of_parti; PartitionCounter=PartitionCounter+1 ))
#do
#  ConformedDirpath=$adlpath$cnfldirpath'/create_date='$v_date'/source_file_name='${DIRNAME[PartitionCounter]}/${DIRNAME[PartitionCounter]}'_'v_conform_job_run_status'.avro'
#done
#End of hive validation

echo CONFORM TO STAGING EXECUTION

GetJobName $SEQ_NUM 'STAGING'
stringarray=($var)
for (( JobCounter=0; JobCounter<${#stringarray[JobCounter]}; JobCounter=JobCounter+1 ))
do
JOBNAME=${stringarray[JobCounter]}


currentdate=`date +%y/%m/%d-%H:%M:%S`
echo currentdate $currentdate


#Getting Source Object name
SOURCE_OBJECT_NAME=$(sed -n "${count} s/^ *export SOURCE_OBJECT_NAME=*//p" $wrapper_directory/$JOBNAME.sh)
echo SOURCE_OBJECT: $SOURCE_OBJECT_NAME

#End of Getting Source Object name

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
SELECT NVL(MAX(JOB_RUN_ID),0), NVL(MIN(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID>$MAX_SRC_JOB_RUN_ID AND TARGET_OBJECT_ID=(SELECT OBJECT_ID FROM JOB_OBJECT_MASTER WHERE OBJECT_NAME='$SOURCE_OBJECT_NAME' AND OBJECT_LAYER='BDCD') AND JOB_RUN_STATUS=1 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
exit;
EOF`

MAX_RUN_ID_CURRENT=$(echo $var | awk -F '[ ]' '{print $1}')
MIN_RUN_ID_CURRENT=$(echo $var | awk -F '[ ]' '{print $2}')

MAX_RUN_ID_CURRENT=$(echo $MAX_RUN_ID_CURRENT | sed -e 's/[\r\n]//g')
echo MAXIMUM SOURCE JOB_RUN_ID CURRENT LOAD: $MAX_RUN_ID_CURRENT

MIN_RUN_ID_CURRENT=$(echo $MIN_RUN_ID_CURRENT | sed -e 's/[\r\n]//g')
echo MINIMUM SOURCE JOB_RUN_ID CURRENT LOAD: $MIN_RUN_ID_CURRENT
#End of Getting MAXIMUM and MINIMUM SOURCE JOB_RUN_ID for current load


if [ $MAX_RUN_ID_CURRENT -eq 0 ] && [ $MIN_RUN_ID_CURRENT -eq 0 ]  #Record availability to process
then
echo NO RECORDS AVAILABLE TO PROCESS
CONFORM_FLAG=1
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
source_query=$(echo $source_query | sed -e "s/\$MIN_RUN_ID_CURRENT/${MIN_RUN_ID_CURRENT}/g")
source_query=$(echo $source_query | sed -e "s/\$MAX_RUN_ID_CURRENT/${MAX_RUN_ID_CURRENT}/g")
echo SOURCE_QUERY AFTER REPLACEMENT: $source_query
#End of Replacing with the actual data


#Getting data for current load from hive table
hive_data=$(hive -e "use governed_data; $source_query;")

hive_data=$(echo "$hive_data" | sed -e 's/[\r\t]/ /g')
hive_data=$(echo "$hive_data" | sed -e 's/ /,/g')
echo HIVE_DATA: "$hive_data"
#End of Getting data for current load from hive table


#JOB_AVAILABILITY in wrappers
if [ $wrapper_directory/$JOBNAME.sh ]  #Checking for job availability
then
echo JOB PRESENT IN THE WRAPPERS
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','CONFORMED TO STAGING','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_AVAILABILITY','JOB_AVAILABILITY IN WRAPPERS','PASSED',SYSTIMESTAMP,'INBOUND');
exit;
EOF

#Executing the job
echo "JOB RUN STARTS: $JOBNAME"
echo "" > nohup.out
nohup sh $wrapper_directory/$JOBNAME.sh
echo "JOB RUN ENDS: $JOBNAME"

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
TargetQuery=($target_query)
echo "TARGETQUERY0=${TargetQuery[0]}"
echo "TARGETQUERY1=${TargetQuery[1]}"

#Getting the data from the staging table
STAGING_DATA=`sqlplus -s $STGDB_CONNECTION <<EOF
set head off feedback off echo off newpage none pagesize 0 linesize 10000 colsep , 
$target_query;
exit;
EOF`

#STAGING_DATA=$(echo "$STAGING_DATA" | sed -e 's/[\r\n]//g')
STAGING_DATA=$(echo "$STAGING_DATA" | sed -e 's/[\r\t]//g')
STAGING_DATA=$(echo "$STAGING_DATA" | sed -e 's/ //g')
echo STAGING DATA: "$STAGING_DATA"
#End of getting data from staging table


#Redirecting source and target data into csv files
echo "$hive_data" > $log_file_path/source_kafka_$job_run_id.csv
echo "$STAGING_DATA" > $log_file_path/target_kafka_$job_run_id.csv
#End of  redirecting source and target data into csv files


#Comparing source data and target data
dif=$(diff -r $log_file_path/source_kafka_$job_run_id.csv $log_file_path/target_kafka_$job_run_id.csv | wc -l)
echo DIFF: $dif
#Comparing source data and target data


#Get the record count in source and target
source_count=$(wc -l < $log_file_path/source_kafka_$job_run_id.csv)
target_count=$(wc -l < $log_file_path/target_kafka_$job_run_id.csv)
echo SOURCE_COUNT: $source_count
echo TARGET_COUNT: $target_count
#Get the record count in source and target


#Comparing data between source and target
echo COMPARISON

if [ $source_count -eq $target_count ]  #Count Validation
then
echo SOURCE AND TARGET COUNT IS MATCHING
  if [ $dif -eq 0 ]  #Data Validation
  then
  echo DATA MATCHING
  CONFORM_FLAG=1
  fi  #Data Validation
else  #Count Validation
CONFORM_FLAG=1
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

echo "$DUPLICATE_DATA" >> $log_file_path/duplicate_kafka_$job_run_id.csv

duplicate_count=$(echo $(sort $log_file_path/duplicate_kafka_$job_run_id.csv | uniq -d))
duplicate_count=$(echo "$duplicate_count" | sed -e 's/[\r\n]//g')


#End of Duplicate check in target


#JOB_PROCESS_CONTROL Check
job_run_status=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT JOB_RUN_STATUS FROM JOB_PROCESS_CONTROL WHERE JOB_RUN_ID=$job_run_id; 
exit;
EOF`

v_job_run_status=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_job_run_status $v_job_run_status

if [ "$v_job_run_status" = "1" ]
then
echo "JOB SUCCESSFUL FOR STAGING"
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,execute_date,interface_type)
values('$SOURCENAME','CONFORMED TO STAGING','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_EXECUTION','JOB_RUN_STATUS','PASSED',SYSTIMESTAMP,'INBOUND');
exit;
EOF

else
#echo JOB FAILED
CONFORM_FLAG=1
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','CONFORMED TO STAGING','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_EXECUTION','JOB_RUN_STATUS','FAILED','JOB_RUN_STATUS is ${job_run_status}',SYSTIMESTAMP,'INBOUND');
exit;
EOF

fi
#JOB_PROCESS_CONTROL Check


else  #Checking for job availability
echo JOB IS NOT AVAILABLE IN THE WRAPPERS
CONFORM_FLAG=1
sqlplus -s $DB_CONNECTION <<EOF
insert into process_owner_test_log(source_name,stage,interface_id,interface_name,interface_subtype,test_scenario,test_case,step_name,test_result,comments,execute_date,interface_type)
values('$SOURCENAME','CONFORMED TO STAGING','$INTERFACEID','$INTERFACENAME','$SUBTYPE','POSITIVE','JOB_AVAILABILITY','JOB_AVAILABILITY IN WRAPPER','FAILED','Job is not present in the wrappers directory',SYSTIMESTAMP,'INBOUND');
exit;
EOF

fi  #Checking for job availability
#End of JOB_AVAILABILITY in wrappers

fi  #Record availability to process
done    #end of partition run loop

echo "STAGING TO 3NF"
valid=0
SEQUENCE=$SEQ_NUM
stagingto3nf $SEQUENCE

