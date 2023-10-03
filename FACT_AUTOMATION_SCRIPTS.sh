BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles"
. $BASE_FOLDER/inbound_environment.sh

currentdate=`date +%y/%m/%d-%H:%M:%S`
echo currentdate $currentdate

var=`sqlplus -s $DB_CONNECTION <<EOF
set head off
SELECT SEQ_NO FROM TESTING_CONFIG_DIM_FACTS;
#SELECT INTERFACE_ID,DIMENTION_FACTS,TABLE_TYPE,JOB_NAME,SOURCE_TABLE,TARGET_TABLE,STAGE FROM TESTING_CONFIG_DIM_FACTS;
exit;
EOF`
SEQ_NO=$var
echo SEQ_NO: $SEQ_NO
 
arr=(`echo ${var}`);
for i in "${arr[@]}"
do
echo $i
EOF`