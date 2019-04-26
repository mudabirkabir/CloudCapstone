#!/bin/bash
EBS_FOLDER=$1
S3_PATH=$2


if [ -d !$EBS_FOLDER]; then
	echo "EBS mounted folder not found"
	exit -1
fi


for FILE in `ls $EBS_FOLDER/airline_ontime/*/*.zip`; do
	for CSV_NAME in `unzip -l $FILE  | grep csv | tr -s ' ' | cut -d ' ' -f4`; do
		unzip -p $FILE $CSV_NAME | aws s3 mv - $S3_PATH/$CSV_NAME
		echo "Moving $CSV_NAME to $S3_PATH"
	done 
done
