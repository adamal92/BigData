# WARNING: before running this script make sure that Hadoop HDFS is installed on your machine and all the relevant
# environment variables are set & configured in PATH env var

# imports
import os, time


# global


class HDFS_handler:
    """
    HDFS commands:

    """
    # static
    HDFS_START_PATH: str = r"C:\Hadoop\hadoop-3.2.1\sbin\start-dfs.cmd"
    HDFS_STOP_PATH: str = r"C:\Hadoop\hadoop-3.2.1\sbin\stop-dfs.cmd"
    START_HDFS: str = r"start " + HDFS_START_PATH
    STOP_HDFS: str = r"start " + HDFS_STOP_PATH
    LIST_ALL: str = "hdfs dfs -ls /"
    HADOOP_USER = "/user/hduser"
    LIST_FILES: str = rf"hdfs dfs -ls {HADOOP_USER}"
    HELP = "hdfs dfs -help"
    MIN_START_TIME: int = 7  # minimum time that hdfs takes to start
    DEFAULT_CLUSTER_PATH: str = "hdfs://localhost:9820/"

    stop = lambda: os.system(HDFS_handler.STOP_HDFS)  # close the hdfs server
    list_all = lambda: os.system(HDFS_handler.LIST_ALL)  # list all users & files
    safemode_off = lambda: os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    safemode_on = lambda: os.system("hdfs dfsadmin -safemode enter")  # safe mode on
    delete_file = lambda filename: \
        os.system(f"hdfs dfs -rm -R -skipTrash {HDFS_handler.HADOOP_USER}/{filename}")  # delete file
    create_file = lambda file_path: os.system(  # create file in hadoop (copy file from local to hadoop)
        f"hdfs dfs -put \"{file_path}\" {HDFS_handler.HADOOP_USER}"
    )
    get_file = lambda hdfs_file_path, local_path: \
        os.system(f"hdfs dfs -copyToLocal \"{hdfs_file_path}\" \"{local_path}\"")  # copy file from hadoop to local
    list_files = lambda: os.system(HDFS_handler.LIST_FILES)
    print_file = lambda file_path: os.system(rf'hdfs dfs -cat {file_path}')
    mkdir = lambda directory: os.system(rf'hdfs dfs -mkdir /{directory}')  # create a directory in hdfs

    @staticmethod
    def start() -> int:
        """
        start the hdfs server
        :return: 0 if succeeded
        """
        result = os.system(HDFS_handler.START_HDFS)
        time.sleep(HDFS_handler.MIN_START_TIME)
        return result
