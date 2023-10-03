BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles"
BASE_FOLDER_FUNCTIONLIBRARY="/home/sshuser/Test_Automation/FunctionLibrary"
. $BASE_FOLDER/KafkaEnvironmentVariables.sh
. $BASE_FOLDER_FUNCTIONLIBRARY/Functions.sh

#SQLConnection DW_STAGING Testing123 10.71.103.6 1521 rdwdev CAMPAIGN
echo $count
currentdate=`date +%y/%m/%d-%H:%M:%S`

SEQ_NUM=$1
SOURCE_STAGE_TestingJOB='STAGING'
SOURCE_STAGE='DW_STAGING'
STAGE='3NF'
#TARGET_OBJECT_ID=317
GetJobName $SEQ_NUM $STAGE 
stringarray=($var)
        
echo $DB_CONNECTION

#GetJobName $SEQ_NUM $STAGE

#stringarray=($var)

counter=0
for (( c=0; c<${#stringarray[@]}; c=c+2 ))
do
echo "Run for JOB ${stringarray[c]}"

  #Get Table Name from Testing Jobs using Job name
  GetTableName ${stringarray[c]}
  TableName[$counter]=$TABLE_NAME
  #If seq is 2 and table name has , then take in 2 variables
  #******
  
  #Get Source Table Name
  GetSourceTableName $SEQ_NUM $SOURCE_STAGE_TestingJOB
  SourceTableName[$counter]=$SOURCE_TABLE_NAME
  
  #Get target Object ID
  GetTargetObjectID ${SourceTableName[$counter]} $SOURCE_STAGE
  TargetObjectID[$counter]=$TARGET_OBJECT_ID
  
  #Get Prev job run ID
  GetJobRunID ${stringarray[c]} $currentdate
  PrevJobRunID[$counter]=$SUCCESS_JOB_RUN_ID
  
  #Get Max Source Job Run ID
  GetMaxSourceJobRunID ${PrevJobRunID[counter]}
  MaxSrcJobRunID[$counter]=$MAX_SRC_JOB_RUN_ID
  
  #Get Max Current Job Run ID
  GetMaxCurrentJobRunID ${TargetObjectID[counter]} ${MaxSrcJobRunID[counter]}
  MaxCurrentJobRunID[$counter]=$MAX_JOB_RUN_ID_CURRENT
  echo "AMax=${MaxCurrentJobRunID[$counter]}"
  
  #Get Min Current Job Run ID
  GetMinCurrentJobRunID ${TargetObjectID[counter]} ${MaxSrcJobRunID[counter]}
  MinCurrentJobRunID[$counter]=$MIN_JOB_RUN_ID_CURRENT
  echo "AMin=${MinCurrentJobRunID[$counter]}"
  
JOBNAME=${stringarray[c]}
#Define the Source queries
source_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(SOURCE_QUERY, dbms_lob.getlength(SOURCE_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

echo SOURCE_QUERY: $source_query
#End of getting the source query


#Replacing with the actual data
source_query=$(echo $source_query | sed -e "s/\$MIN_RUN_ID_CURRENT/${MIN_JOB_RUN_ID_CURRENT}/g")
SourceSQLQuery_Data=$(echo $source_query | sed -e "s/\$MAX_RUN_ID_CURRENT/${MAX_JOB_RUN_ID_CURRENT}/g")
echo SOURCE_QUERY AFTER REPLACEMENT: $SourceSQLQuery_Data

echo $SourceSQLQuery_Data
echo "$SourceSQLQuery_Data" >> $log_file_path/source_kafka_$job_run_id.csv

#Get count from Source Tables
DataCount=$(wc -l < $log_file_path/source_kafka_$job_run_id.csv)
ExpDataCount[$counter]=$DataCount
echo "Exp Count=${ExpDataCount[counter]}"

#Get Data from Source table
Data=`sqlplus -s $STGDB_CONNECTION << EOF
set head off 
$SourceSQLQuery_Data;
exit;
EOF`
ExpData[$counter]=$Data
echo "Exp Data=${ExpData[counter]}"

# Run Jobs
#sh /insights/app/scripts/wrappers/${stringarray[c]}.sh
JOBNAME=${stringarray[c]}
echo "getting data after running job"

#Run Current Job run ID
GetJobRunID ${stringarray[c]} $currentdate
CurrentJobRunID[$counter]=$SUCCESS_JOB_RUN_ID

#Define the Target queries
target_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(TARGET_QUERY, dbms_lob.getlength(TARGET_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

echo TARGET_QUERY: $target_query
#End of Getting the target query

#Replacing with the actual values
TargetSQLQuery_Data=$(echo $target_query | sed -e "s/\$job_run_id/${SUCCESS_JOB_RUN_ID}/g")

echo TARGET_QUERY AFTER REPLACEMENT: "$TargetSQLQuery_Data"
echo $TargetSQLQuery_Data

echo "$TargetSQLQuery_Data" >> $log_file_path/target_kafka_$job_run_id.csv

#Get count from Target Tables
DataCount=$(wc -l < $log_file_path/target_kafka_$job_run_id.csv)
ActDataCount[$counter]=$DataCount
echo "Real Count for table ${TableName[$counter]} is ${ActDataCount[counter]}"

#get Data from Target tables by running the queries
#Get count from Target Tables
Data=`sqlplus -s $TNFDB_CONNECTION << EOF
set head off 
$TargetSQLQuery_Data;
exit;
EOF`
ActData[$counter]=$Data
echo "Act Data for table ${TableName[$counter]} is ${ActData[counter]}"

counter=`expr $counter + 1`
done

#Compare both the data
for (( i=0; i<"${#ActDataCount[@]}"; i++ ))
do
  if [ "${ExpDataCount[$i]}" == "${ActDataCount[$i]}" ]
  then
    echo "Pass-Both Count is same"
  else
      echo "Fail-Both Count is not same"      
  fi
  
  if [ "${ExpData[$i]}" == "${ActData[$i]}" ]
  then
    echo "Pass-Both Data is same"
  else
      echo "Fail-Both data is not same"      
  fi
done