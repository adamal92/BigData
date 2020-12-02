try
{
    # Do your script's stuff
	
	start C:\Hadoop\hadoop-3.2.1\sbin\start-dfs.cmd  # run hdfs
	Start-Sleep 10  # wait for hdfs to start
	hdfs dfs -ls /user/hduser  # list all files that user has

	pause  # make the console wait for user before closing
}
catch
{
	Write-Error $_.Exception.ToString()
	Read-Host -Prompt "The above error occurred. Press Enter to exit."
}

# powershell.exe -ExecutionPolicy Bypass -File "C:\Users\adam l\Desktop\hdfs_script.ps1"
# Get-ExecutionPolicy  # Bypass
# Set-ExecutionPolicy RemoteSigned
#  & "C:\Users\adam l\Desktop\hdfs_script.ps1"