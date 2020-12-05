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
    LIST_FILES: str = r"hdfs dfs -ls /user/hduser"
    HELP = "hdfs dfs -help"
    MIN_START_TIME: int = 7  # minimum time that hdfs takes to start
    DEFAULT_CLUSTER_PATH: str = "hdfs://localhost:9820/"

    stop = lambda : os.system(HDFS_handler.STOP_HDFS)

    @staticmethod
    def start() -> int:
        result = os.system(HDFS_handler.START_HDFS)
        time.sleep(HDFS_handler.MIN_START_TIME)
        return result
