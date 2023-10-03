BASE_FOLDER="/home/sshuser/Test_Automation/ConfigFiles"
BASE_FOLDER_FUNCTIONLIBRARY="/home/sshuser/Test_Automation/FunctionLibrary"
BASE_FOLDER_SCRIPTS="/home/sshuser/Test_Automation/Scripts"
. $BASE_FOLDER/inbound_environment.sh
. $BASE_FOLDER_FUNCTIONLIBRARY/kafka_Stagingto3NF.sh


valid=0
SEQUENCE=$1
stagingto3nf $SEQUENCE

