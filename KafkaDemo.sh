#!/bin/bash
# Parameters
# ==========
# TOPICNAME        		char  Topic Name to be tested
# SOURCENAME            char  Source system to be tested
# TESTCASE        		char  Test scenario to be tested
# Expectedstatus		numeric 1,2 or 3


BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles/"
. $BASE_FOLDER/KafkaEnvironmentVariables.sh


#customer_incident
TOPICNAME='customer_incident'
v_interfacepath='/root/insights/landing/crm/5.10.7.6.c/itsm_ticket_details'

stringarray[0]="PY_ESTL_FPTL_5_10_7_6_c_itsm_ticket_details_0"
stringarray[1]="PY_ESTL_FPTL_5_10_7_6_c_itsm_ticket_details_1"
JobArray[0]=PY_ESTL_FPTL_5_10_7_6_c_itsm_ticket_details_0.sh
JobArray[1]=PY_ESTL_FPTL_5_10_7_6_c_itsm_ticket_details_1.sh

v_path=$adlpath$v_interfacepath
Expectedstatus=1
TESTCASE='Kafka validation'


#exec > /home/sshuser/Ajit_work/Log.sh
currentdate=`date +%y/%m/%d-%H:%M:%S`
echo "cita db: $DB_CONNECTION"
#query to take the number of partitions from EVENT_CONTROL_CONFIG table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select number_of_partitions from EVENT_CONTROL_CONFIG where TOPIC_NAME = '$TOPICNAME'; 
exit;
EOF
)

#storing the number of partitions retreived from EVENT_CONTROL_CONFIG table to local variables
v_nm_of_parti=$(echo $variable | awk -F '[ ]' '{print $1}')
echo v_nm_of_parti $v_nm_of_parti

FinalCount=0
act_file_data=""
for (( PartitionCounter=0; PartitionCounter<$v_nm_of_parti; PartitionCounter=PartitionCounter+1 ))
do
#query to select job_id from job_master table
SOURCENAME=${stringarray[PartitionCounter]}
echo "SOURCENAME=$SOURCENAME"
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
select (MAX(to_number(UNTIL_OFFSET))) from kafka_event where TOPIC_NAME='$TOPICNAME'; 
exit;
EOF
)
prev_untilOffset=$(echo $variable | awk -F '[ ]' '{print $1}')
echo "prev_untilOffset $prev_untilOffset"


#Run the job

JOB=${JobArray[PartitionCounter]}
echo "" > nohup.out
nohup sh /insights/app/scripts/wrappers/$JOB

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
echo "TimeDiff is $TimeDiff"

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

#query to take the from Offset from kafka_event table
variable=$(sqlplus -s $DB_CONNECTION <<EOF
set pages 0 echo off feed off head off
select (MAX(to_number(from_offset))) from kafka_event where topic_name ='$TOPICNAME';
exit;
EOF
)

#storing the count retreived from kafka_event table to local variables
new_from_offset=$(echo $variable | awk -F '[ ]' '{print $1}')
echo new_from_offset $new_from_offset

#Validate if untill offset for prev job run is same as from offset for current job run
TESTCASE='validate offset starts from previous end offset'
Expectedstatus='Prev until offset equal to current from offset'
if [ "$prev_untilOffset" = "$new_from_offset" ]
then
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','PASSED','The offset are as expected',sysdate); 
exit;
EOF
else
sqlplus -s $DB_CONNECTION <<EOF
insert into HADOOP_TOPIC_TEST_LOG(source_name,testcase,TOPICNAME,Expectedstatus,TEST_RESULT,comments,EXECUTE_DATE)
values('$SOURCENAME','$TESTCASE','$TOPICNAME','$Expectedstatus','FAILED','The offset are not as expected',sysdate); 
exit;
EOF
fi

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
      
      FileName=$(hadoop fs -ls $v_path'/process/'$DIR_NAME)
      echo "FileName: $FileName" 
           
      FileCount=$(echo $FileName | awk -F $DIR_NAME '{print NF}')
      FileCount=(`expr $FileCount - 2`)
      echo "FileCount: $FileCount"
      
      FileName1=$(echo $FileName | awk -F $DIR_NAME '{print $3}')
      echo "FileName1: $FileName1"
      
			#if the file is present for a particular partition check the count and add it up to arrive the total count
			if [ 0 ]
			then
				v_file_counter=$(hadoop fs -cat $v_path'/process/'$DIR_NAME/part-00000 | wc -l)
        v_file_data=$(hadoop fs -cat $v_path'/process/'$DIR_NAME/part-00000)
        echo "v_file_counter: $v_file_counter"
        echo "v_file_data: $v_file_data"
        act_file_data=$act_file_data"\n"$v_file_data
        echo "act_file_data: $act_file_data"
        
				v_file_data_count=(`expr $v_file_counter + $v_file_data_count`)
        echo "v_file_data_count: $v_file_data_count"
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
fi
	fi
fi
done

echo "FinalCount $FinalCount"
echo "Act_FinalCount $Act_FinalCount"

#Validate the merged count
if [ "$FinalCount" = "$Act_FinalCount" ]
then
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