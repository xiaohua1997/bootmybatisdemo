package com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban2;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.ext.azkaban.plugin.AzkabanShell;
import com.moumou.bootmybatisdemo.dataAlignment.model.SourceTable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class HiveJob extends Job {
    @Override
    public List<String> appendJobsToFlowFile(List<SourceTable> lstTable, HashMap<String, String> schemaNumMap, HashMap<String, String> tableNameConvertMap) throws IOException {
        return null;
    }



    public static HashMap<String, String> dicTopicInfo;
    static  {
        dicTopicInfo = new HashMap<>();
        dicTopicInfo.put("cp","ChanPing"); //产品
        dicTopicInfo.put("gz","GuZhi"); //估值
        dicTopicInfo.put("jj","JingJi"); //经纪
        dicTopicInfo.put("kh","KeHu");//客户
        dicTopicInfo.put("th","TouHang"); //投行
        dicTopicInfo.put("xy","XinYong"); //信用
        dicTopicInfo.put("zg","ZiGuan"); //资管
        dicTopicInfo.put("zh","ZhangHu"); //账户
        dicTopicInfo.put("zs","ZuoShi"); //做市
        dicTopicInfo.put("zy","ZiYing"); //自营
        dicTopicInfo.put("zz","ZuZhi"); //组织
    }

    public void hiveJob() throws IOException {
        // 接收脚本目录，进行文件名扫描（不包括子目录）
        File scriptDir = new File("C:\\Users\\Administrator\\Desktop\\五矿svn\\02 项目实施\\0203 系统开发\\020308 Hive_iml_sql");
        if(!scriptDir.isDirectory()) {
            System.out.println("不是目录");
            return;
        }
        Collection<File> allFiles = FileUtils.listFiles(scriptDir, FileFilterUtils.suffixFileFilter(".q"), null);
        System.out.println(allFiles.size());
        // 分析文件名，进行主题分类
        HashMap<String, List<File>> dicHql = new HashMap<>();
        for (File aFile : allFiles) {
            String name = aFile.getName();
            String[] fragments = name.split("_");
            if(fragments.length < 2) {
                System.out.println("跳过：" + aFile.getName());
                continue;
            }
            String topicShortName = fragments[1];
            if(!dicTopicInfo.containsKey(topicShortName)) {
                System.out.println("跳过：" + aFile.getName());
                continue;
            }
            if(dicHql.containsKey(topicShortName)) {
                List<File> lstTopicFiles = dicHql.get(topicShortName);
                lstTopicFiles.add(aFile);
            } else {
                List<File> lstTopicFiles = new ArrayList<>();
                lstTopicFiles.add(aFile);
                dicHql.put(topicShortName, lstTopicFiles);
            }
        }

        AzkabanShell azkabanShell = new AzkabanShell();
        // 循环处理每个主题
        for (String shortTopicName : dicHql.keySet()) {
            String fullTopicName = dicTopicInfo.get(shortTopicName);
            List<File> lstFiles = dicHql.get(shortTopicName);
            // 生成文件夹
            String targetDir = URLDecoder.decode(HiveJob.class.getClassLoader().getResource("").getPath(), "utf-8")
                    + "/datawarehouse_dwd_" + fullTopicName;
            System.out.println(targetDir);
            // 生成基本文件和工具文件
            HiveJob aHiveJob = new HiveJob(targetDir,"datawarehouse_dwd_" + fullTopicName);
            aHiveJob.createProjectFile(true);
            azkabanShell.generateHiveShell(targetDir);
            // 生成flow
            aHiveJob.createFlowFile(true, true);
            //
            File hiveScriptDir = new File(targetDir + "/dwd/");
            // 循环文件列表，将其job写入flow
            int cnt = 1;
            for(File aFile : lstFiles) {
                String jobName = FilenameUtils.getBaseName(aFile.getName());
                String command = "bash ./call_hive.sh ${para_date} "
                              + "dwd "
                              + FilenameUtils.getBaseName(aFile.getName()) + " "
                              + "${check} "
                              + "${check_interval_minutes} "
                              + "${check_max_loop}";
                 aHiveJob.appendJob(jobName, "command", command, null);
                 //复制.q文件到目标目录下的dwd目录
                 FileUtils.copyFileToDirectory(aFile, hiveScriptDir, true);
                 cnt++;
            }

            // 成功信息
            System.out.println(fullTopicName + " OK, File Count=" + cnt);
        }
        System.out.println("OK");
    }

    public HiveJob(String dir, String flowName) {
        super(dir, flowName);
    }
}
