#!/bin/bash
# 0.1 where error exit
# set -e

# show command
#set -v

# show run command
#set -x

#su - dw_test01

# parameter define
# if param1 equals 'N' is true, set date by T-1
if [ "${1}" == "N" ];then
    batch_date=`date -d "yesterday" +%Y%m%d`
else
    batch_date=${1}
    check_batch_date=`echo ${batch_date} | sed -n '/^[1-3][0-9][0-9][0-9][0-1][0-9][0-3][0-9]$/p'`
    if [ ! $check_batch_date ];then 
        echo "[ERROR]The batch date parameter is not valid and must be in YYYYMMDD format."
        exit 255
    fi
fi
echo "batch date is: ${batch_date}"

#data_date=${batch_date:0:4}-${batch_date:4:2}-${batch_date:6:2}
data_date=${batch_date}

#etl_time=`date +"%Y-%m-%d %H:%M:%S"`
etl_time1=`date +%Y-%m-%d`
etl_time2=`date +%H:%M:%S`

src_sys=${2}
src_schema=${3}
src_table_name=${4}
tgt_table=${5}

# EtlAssistPro auto generate source system information
connect="${EtlAssistPro:jdbcurl}"
src_username="${EtlAssistPro:usr}"
src_password="${EtlAssistPro:pwd}"
# Impala information
tgt_ip="hadoop04.wkzq.com"
tgt_port="21000"
tgt_schema="datawarehouse01"
#tgt_table=${src_sys}_${src_table_name}
tgt_pk="part_ymd"
tgt_pv=${batch_date}
queue="default"

job_name=${tgt_table/"ods_"/"ods_e_"}
echo "job_name=${job_name}"
# search job_date_offset.conf by jobname
# if exists, modify batch_date with offset
offsetfile="job_date_offset.conf"
if [[ -f "${offsetfile}" ]]; then
    date_offset=`cat ${offsetfile} | grep "${job_name}=" | awk -F "=" {'print $2'}`
    if [ $date_offset ];then 
        echo "job date offset mode: ${date_offset}"
        batch_date=`date -d "${batch_date} ${date_offset} day" +%Y%m%d`
    else
        echo "normal mode"
    fi
fi
echo "new batch date is: ${batch_date}"

# white.calendar exists?
#   Y -> batch_date exitst?
#     Y -> continue run(break black.calendar)
#     N -> exit(0)
#   N -> continue run
breakblack_flag="FALSE"
whitefile="white.calendar"
if [[ -f "${whitefile}" ]]; then
    white_content=`cat ${whitefile} | grep "${batch_date}"`
    if [ $white_content ];then 
        breakblack_flag="TRUE"
        echo "${batch_date} exists in ${whitefile}. Continue."
    else
        echo "${batch_date} not exists in ${whitefile}. Program Exit 0."
        exit 0
    fi
else
    echo "NO WHITE DATE FILE. continue run."
fi

# black.calendar exists?
#  Y -> batch_date exists?
#    Y -> exit(0)
#    N -> continue run
#  N -> continue run
blackfile="black.calendar"
if [[ -f "${blackfile}" && "FALSE" = ${breakblack_flag} ]]; then
    black_content=`cat ${blackfile} | grep "${batch_date}"`
    if [ $black_content ];then 
        echo "${batch_date} exists in ${blackfile}. Program Exit 0."
        exit 0
    else
        echo "${batch_date} not exists in ${blackfile}. Continue."
    fi
else
    if [ "FALSE" = ${breakblack_flag} ];then
        echo "NO BLACK DATE FILE. continue run."
    else 
        echo "${batch_date} exists in ${whitefile}. Break BLACK DATE FILE."
    fi
fi


# 0.2 prepare file full path
#sql_file=./sqoop/${src_sys}/${src_sys}_${src_table_name}.sql
sql_file=./sqoop/${src_sys}/${tgt_table}.sql

# 1.0 get ext sql
ext_sql=`\
    cat $sql_file | \
    sed s'/${src_schema}/'''${src_schema}'''/g' | \
    sed s'/${batch_date}/'''${batch_date}'''/g' | \
    sed s'/${data_date}/'''${data_date}'''/g' | \
    sed s'/${etl_time}/'''"${etl_time1} ${etl_time2}"'''/g'
`

# keytab login
# kinit -k -t /home/etltool/etlscript/keytab/etluser.keytab etluser

# 1.1 drop table partition
partition_drop=`
impala-shell -i ${tgt_ip}:${tgt_port} -d ${tgt_schema} \
-l --auth_creds_ok_in_clear -u dw_test01 --ldap_password_cmd="echo -n dwtest88@wkzq" \
-q "
alter table ${tgt_schema}.${tgt_table} drop if exists partition (${tgt_pk} = '${tgt_pv}');
" 2>&1
`

drop_status=$?
echo "$partition_drop"

if [ "$drop_status" == "0" ];then
    echo -e "succeeded drop partition"
else
    echo -e "$partition_drop"
    exit $drop_status
fi

# 1.2 call sqoop, put stdout and stderr to dat_out_err
if [ $src_password ];then
	dat_out_err=`
	export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
	sudo -u dw_test01 sqoop import -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
		-D mapred.job.queue.name=$queue \
		--connect $connect \
		--username ${src_username} \
		--password ${src_password} \
		--query "$ext_sql and \\\$CONDITIONS" \
		--hcatalog-database ${tgt_schema} \
		--hcatalog-table ${tgt_table} \
		--hcatalog-partition-keys ${tgt_pk} \
		--hcatalog-partition-values ${tgt_pv} \
		-m 1 2>&1
	`
else
dat_out_err=`
	export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
	sudo -u dw_test01 sqoop import -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
		-D mapred.job.queue.name=$queue \
		--connect $connect \
		--username ${src_username} \
		--query "$ext_sql and \\\$CONDITIONS" \
		--hcatalog-database ${tgt_schema} \
		--hcatalog-table ${tgt_table} \
		--hcatalog-partition-keys ${tgt_pk} \
		--hcatalog-partition-values ${tgt_pv} \
		-m 1 2>&1
	`
fi

exec_status=$?
echo "stdout: $dat_out_err"

# 1.3 refresh table metadata
table_refresh=`
impala-shell -i ${tgt_ip}:${tgt_port} -d ${tgt_schema} \
-l --auth_creds_ok_in_clear -u dw_test01 --ldap_password_cmd="echo -n dwtest88@wkzq" \
-q "
invalidate metadata ${tgt_schema}.${tgt_table};
" 2>&1
`

refresh_status=$?
echo "$table_refresh"

# 4 final status
if [ "$exec_status" == "0" ];then
    if [ "$refresh_status" == "0" ];then
        echo -e "succeeded load data and refresh table"
    else
        echo -e "$table_refresh"
        exit $refresh_status
    fi
else
    echo -e "$dat_out_err"
    exit $exec_status
fi
