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

tgt_table=${2}

# Hadoop information
hive_ip="192.10.40.161"
hive_port="10000"
impala_ip="hadoop04.wkzq.com"
impala_port="21000"
database="datawarehouse01"
queue="default"
usr="dw_test01"
pwd="dwtest88@wkzq"

job_name=${tgt_table}
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


# 0.2 prepare file full pat
sql_file=./hive/${tgt_table}.q

# 1.0 get ext sql
#ext_sql=`\
#    cat $sql_file | \
#    sed s'/${src_schema}/'''${src_schema}'''/g' | \
#    sed s'/${batch_date}/'''${batch_date}'''/g' | \
#    sed s'/${data_date}/'''${data_date}'''/g' | \
#    sed s'/${etl_time}/'''"${etl_time1} ${etl_time2}"'''/g'
#`

# keytab login
# kinit -k -t /home/etltool/etlscript/keytab/etluser.keytab etluser

# Execute Hive Script File By Beeline
dat_out_err=`
beeline -u jdbc:hive2://${hive_ip}:${hive_port}/${database} \
-n ${usr} \
-p ${pwd} \
--hivevar batch_date=${batch_date} \
-f ${sql_file} 2>&1
`

exec_status=$?
echo "stdout: $dat_out_err"

# sleep a moment, wait HiveMeta Sync Finish.
sleep 30s

# 1.3 refresh table metadata on impala
table_refresh=`
impala-shell -i ${impala_ip}:${impala_port} -d ${database} \
-l --auth_creds_ok_in_clear -u ${usr} --ldap_password_cmd="echo -n ${pwd}" \
-q "
invalidate metadata ${database}.${tgt_table};
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
