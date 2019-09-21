#!/bin/bash
# set -x
# parameter define
# if param1 equals 'N' is true, set date by T-1
if [ "${1}" == "N" ];then
    batch_date=`date -d "yesterday" +%Y%m%d`
else
    batch_date="${1}"
    check_batch_date=`echo ${batch_date} | sed -n '/^[1-3][0-9][0-9][0-9][0-1][0-9][0-3][0-9]$/p'`
    if [ ! $check_batch_date ];then 
        echo "[ERROR]The batch date parameter is not valid and must be in YYYYMMDD format."
        exit 255
    fi
fi
echo "batch date is: ${batch_date}"

job_name="${2}"
echo "job_name=${job_name}"

dsConfPath="${3}"
sqlPath="${4}"
checkMode="${5}"
sleepSecond="${6}"
srcKeyValueStr="${7}"
echo "srcKeyValueStr=${srcKeyValueStr}"
expectValue="${8}"
echo "expectValue=${expectValue}"

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

# 处理非固定值类型的活动预期值参数，@{sys:batch_date}代表使用跑批日期作为expectValue
if [ "${expectValue}" == "@{sys:batch_date}" ]; then
    expectValue="${batch_date}"
    echo "convert expectValue=${expectValue}"
fi

DBCHECK_HOME_PATH=`sed '/^DBCHECK_HOME_PATH=/!d;s/.*=//' call_dbcheck.conf`    

# 这里只是显示用，由于参数空格问题，直接dat_out_err=`${ext_cmd} 1>&2` 会先计算ext_cmd内部表达式，此时双引号未实际发生作用，导致带空格的被自动拆分
show_cmd="java -jar ${DBCHECK_HOME_PATH}/DbCheck.jar ${dsConfPath} ${sqlPath} ${checkMode} ${sleepSecond} \"${srcKeyValueStr}\" \"${expectValue}\""
echo ${show_cmd}

# 真正执行。将shell的stdout定位到shell的stderr，这样才能实时滚动显示java打印的信息
dat_out_err=`java -jar ${DBCHECK_HOME_PATH}/DbCheck.jar ${dsConfPath} ${sqlPath} ${checkMode} ${sleepSecond} "${srcKeyValueStr}" "${expectValue}" 1>&2`

# 1.3 get call return code
dat_result=$?

# 1.4 print the log
echo -e "$dat_out_err"

echo "dat_result=${dat_result}"
exit ${dat_result}