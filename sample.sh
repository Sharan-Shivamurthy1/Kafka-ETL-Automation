


IFS=":"    #Field Seperator
#Loop to get the data from txt file
while read f1 f2 f3 f4
do
job=$f1
echo JOB IS: $job
#if [ "$job" == "$JOBNAME" ]    #if statement for job name comparison to take data from the csv file
#then
#echo JOBNAME: $job
sql=$f2
echo QUERY IS: $sql
target=$f3
echo TARGET: $target
null=$f4
echo NULL: $null

#fi    #End of if statement for job name comparison to take data from the csv file
done < query1.csv
#Loop to get the data from txt file
unset IFS