#!/bin/bash

startDate=''
endDate=''

until [ $# -eq 0 ]
do
	if [ $1'x' = '-sdx' ]; then
		shift
		startDate=$1
	elif [ $1'x' = '-edx' ]; then
		shift
		endDate=$1
	fi
	shift
done

if [ -n "$startDate" ] && [ -n "$endDate" ]; then
	echo "use the arguments of the date"
else
	echo "use the default date"
	startDate=$(date -d last-day +%Y-%m-%d)
	endDate=$(date +%Y-%m-%d)
fi
echo "run of arguments. start date is:$startDate, end date is:$endDate"
echo "start run of event job "

## insert overwrite
echo "start insert event data to hive table"
hive --database bigdater -e "with tmp as (select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,ca,ac from event_logs where en='e_e' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000 ) from (select pl as pl,date,ca as ca,ac as ac,count(1) as times from tmp group by pl,date,ca,ac union all select 'all' as pl,date,ca as ca,ac as ac,count(1) as times from tmp group by date,ca,ac union all select pl as pl,date,ca as ca,'all' as ac,count(1) as times from tmp group by pl,date,ca union all select 'all' as pl,date,ca as ca,'all' as ac,count(1) as times from tmp group by date,ca union all select pl as pl,date,'all' as ca,'all' as ac,count(1) as times from tmp group by pl,date union all select 'all' as pl,date,'all' as ca,'all' as ac,count(1) as times from tmp group by date) as tmp2 insert overwrite table stats_event select platform_convert(pl),date_convert(date),event_convert(ca,ac),sum(times),date group by pl,date,ca,ac"

## sqoop
echo "run the sqoop script,insert hive data to mysql table"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_event --export-dir /hive/bigdater.db/stats_event/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,data_dimension_id,event_dimension_id
echo "complete run the event job"