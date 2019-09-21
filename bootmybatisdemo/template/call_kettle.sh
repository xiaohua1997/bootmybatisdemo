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

src_sys=${2}
src_schema_num=${3}
src_table_name=${4}

if [ "$src_schema_num" == "0" ];then
    job_name="ods_e_${src_sys}_${src_table_name}"
else
    job_name="ods_e_${src_sys}_${src_schema_num}_${src_table_name}"
fi

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

# 0.1 where error exit
# set -e

# show command
#set -v

# show run command
#set -x

# 0.2 prepare file full path
script_abs=$(readlink -f "$0")
script_dir=$(dirname $script_abs)

if [ "$src_schema_num" == "0" ];then
    sql_file=${script_dir}/kettle/${src_sys}/${src_sys}_${src_table_name}.xml
else
    sql_file=${script_dir}/kettle/${src_sys}/${src_sys}_${src_schema_num}_${src_table_name}.xml
fi

KETTLE_HOME_PATH=`sed '/^KETTLE_HOME_PATH=/!d;s/.*=//' call_kettle.conf`    

chmod -R 777 ${KETTLE_HOME_PATH}/system/karaf

# call kettle
ext_cmd="
    bash ${KETTLE_HOME_PATH}/pan.sh -file=${sql_file} \
    -param batch_date=${batch_date} \
    2>&1
"

echo $ext_cmd
dat_out_err=`$ext_cmd`

# 1.3 get kettle call return code
dat_result=$?

# 1.4 print the log
echo -e "$dat_out_err"

# 2.0 check the log
tableinputstatus=`echo -e "$dat_out_err" | grep "Step " | awk -F "ended " {'print $2'}| awk -F "," {'print $1'}`
#operationstatus=`echo -e "$dat_out_err" | grep "Step Table input" | awk -F "." {'print $2'}| awk {'print $3'}| awk -F "," {'print $1'}`
#fileoutputstatus=`echo -e "$dat_out_err" | grep "Step Table input" | awk -F "." {'print $2'}| awk {'print $3'}| awk -F "," {'print $1'}`

echo $tableinputstatus
#echo $operationstatus
#echo $fileoutputstatus
 
#if [ "$tableinputstatus" == "successfully" ] && [ "$operationstatus" == "successfully" ] && [ "$fileoutputstatus" == "successfully"  ] ;then
if [ "$tableinputstatus" == "successfully
successfully" ] ;then
    exec_status="0"
else
    exec_status="255"
fi

if [ "$exec_status" -eq "0" ] ;then
    echo "extract data using kettle success"
    exit 0
else
    echo "extract data using kettle falied: $exec_status"
    exit 255
fi