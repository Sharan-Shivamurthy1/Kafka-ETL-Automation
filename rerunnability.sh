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




#TYPE OF TABLE
typeinfo=`awk -F, '{print $2,$3}' DIM_mapping_summary.csv`
typeinfo=`echo $typeinfo|sed 's/Table Name Table Type //'`
echo Type Info $typeinfo

#exit
tbl=`awk -F, '{print $2}' *.csv`
tbl=`echo $tbl|sed 's/ /:/g'`

IFS=':' read -r -a tblarr <<< "$tbl"

for i in "${tblarr[@]}"
do
	#Code to skip blank elements
	if [ $i == '' ]; then
		continue
	fi
	#code to determine DIM/FACT table
	if [[ $i == *DIM* ]]; then
		tempi="${i} Type 1"
		tempi1="${i} Static"
		
		if [[ $typeinfo =~ $tempi1 ]]; then
                        echo It is a Dimension table $i == static
		elif [[ $typeinfo =~ $tempi ]]; then
			echo It is a Dimension table $i == type 1
			
			############## Type1 #################
	
		else
			echo It is a Dimension table $i == type 2 
       #Getting Previous Successful JOB_RUN_ID
PRE_SUCCESS_JOB_RUN_ID=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
exit;
EOF`
#Changing the reprocess_flag to 1
sqlplus -s $DB_CONNECTION <<EOF
UPDATE JOB_PROCESS_CONTROL SET REPROCESS_FLAG=1 WHERE job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
commit;
exit;
EOF

target_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(TARGET_QUERY, dbms_lob.getlength(TARGET_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

rm_where=$(echo $target_query | awk -F ' WHERE' '{print $1}')

data=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
$rm_where where create_job_run_id=$PRE_SUCCESS_JOB_RUN_ID or update_job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`

echo data to be removed $data


records_update_data=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
$rm_where where update_job_run_id=$PRE_SUCCESS_JOB_RUN_ID or record_satus=0;
exit;
EOF`

#Executing the job
echo Job Starts................
sh $wrapper_directory/$JOBNAME.sh
result=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
$rm_where where create_job_run_id=$PRE_SUCCESS_JOB_RUN_ID or update_job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`


echo result $result

if [ "$result" == "" ]
then
echo data is deleted
else
echo data is not deleted
fi
      
		fi
	elif [[ $i == *FACT* ]] || [[ $i == *DAGG* ]]; then
 #Getting Previous Successful JOB_RUN_ID
PRE_SUCCESS_JOB_RUN_ID=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
SELECT NVL(MAX(JOB_RUN_ID),0) FROM JOB_PROCESS_CONTROL WHERE JOB_ID=(SELECT JOB_ID FROM JOB_MASTER WHERE JOB_NAME='$JOBNAME') AND JOB_RUN_STATUS=1 AND REPROCESS_FLAG=0 AND JOB_END_DATE_TIME>=TO_DATE('$currentdate','YYYY-MM-DD HH24:MI:SS');
exit;
EOF`
#Changing the reprocess_flag to 1
sqlplus -s $DB_CONNECTION <<EOF
UPDATE JOB_PROCESS_CONTROL SET REPROCESS_FLAG=1 WHERE job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
commit;
exit;
EOF

target_query=`sqlplus -s $DB_CONNECTION <<EOF
set head off feedback off linesize 10000
SELECT dbms_lob.substr(TARGET_QUERY, dbms_lob.getlength(TARGET_QUERY), 1) FROM TESTING_QUERIES WHERE JOB_NAME='$JOBNAME';
exit;
EOF`

rm_where=$(echo $target_query | awk -F ' WHERE' '{print $1}')

data=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
$rm_where where create_job_run_id=$PRE_SUCCESS_JOB_RUN_ID or update_job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`

echo data to be removed $data

#Executing the job
echo Job Starts................
sh $wrapper_directory/$JOBNAME.sh
result=`sqlplus -s $DB_CONNECTION <<EOF
set head off 
$rm_where where create_job_run_id=$PRE_SUCCESS_JOB_RUN_ID or update_job_run_id=$PRE_SUCCESS_JOB_RUN_ID;
exit;
EOF`


echo result $result

if [ "$result" == "" ]
then
echo data is deleted
else
echo data is not deleted
fi


		echo FACT Table $i
   
	fi
done