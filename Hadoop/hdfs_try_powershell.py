
# print(os.system(r"cd C:\Hadoop\hadoop-3.2.1\sbin"))
#print(os.system("powershell.exe -File \"C:\\Users\\adam l\\Desktop\\hdfs_script.ps1\""))

# WARNING: before running this you need to set the execution policy
# by running this command as administrator on powershell: Set-ExecutionPolicy RemoteSigned

# way 1
import os, time

print(os.system(r"start C:\Hadoop\hadoop-3.2.1\sbin\start-dfs.cmd"))
time.sleep(10)
print(os.system(r"hdfs dfs -ls /user/hduser"))  # : make this work

# way 2
import subprocess, sys

p = subprocess.Popen(["powershell.exe", "-ExecutionPolicy", "Bypass", "-File",
                      "C:\\Users\\adam l\\Desktop\\python files\\BigData\\hdfs_script.ps1"], stdout=sys.stdout)
p.communicate()

# way 3
print(os.system("powershell.exe -ExecutionPolicy"
                " Bypass -File \"C:\\Users\\adam l\\Desktop\\python files\\BigData\\hdfs_script.ps1\""))

r"""
hdfs
hdfs dfs -ls /
 hdfs dfs -ls /user/hduser
 hdfs dfs -cat /user/hduser/first_sqlite_db.db
  hdfs dfs -cat first_sqlite_db.db
  hadoop checknative -a
hdfs dfs -put "C:/cyber/PortableApps/SQLiteDatabaseBrowserPortable/first_sqlite_db.db" /user/hduser
hdfs dfs -ls -R /user
hdfs dfs -mkdir /user/hduser
.\start-dfs.cmd
.\start-yarn.cmd
 cd "C:\Hadoop\hadoop-3.2.1\sbin"
 start C:\Hadoop\hadoop-3.2.1\sbin\start-dfs.cmd
 spark-submit "C:\Users\adam l\Desktop\python files\BigData\pyspark_first.py"
hdfs namenode -format
jps
spark-shell
pyspark

"""

import os, time

print(os.system(r"start C:\Hadoop\hadoop-3.2.1\sbin\start-dfs.cmd"))
time.sleep(7)  # minimum time that hdfs takes to start
print(os.system(r"hdfs dfs -ls /user/hduser"))
print(os.system("hdfs dfs -ls /"))
print(os.system("hdfs dfs -help"))