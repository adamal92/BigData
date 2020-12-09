# Code taken from
# https://stackoverflow.com/questions/19672352/how-to-run-python-script-with-elevated-privilege-on-windows

# !/usr/bin/env python
# -*- coding: utf-8; mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vim: fileencoding=utf-8 tabstop=4 expandtab shiftwidth=4

# (C) COPYRIGHT © Preston Landers 2010
# Released under the same license as Python 2.6.5

"""
This file starts itself in administrative mode
"""

import sys, os, traceback, types


class Admin_Handler:
    # @staticmethod
    # def start_function(func):
    #     # func()
    #     def wrapper():
    #         fn = func()
    #         return print(fn)
    #
    #     return wrapper

    @staticmethod
    def isUserAdmin():
        if os.name == 'nt':
            import ctypes
            # WARNING: requires Windows XP SP2 or higher!
            try:
                return ctypes.windll.shell32.IsUserAnAdmin()
            except:
                traceback.print_exc()
                print("Admin check failed, assuming not an admin.")
                return False
        elif os.name == 'posix':
            # Check for root on Posix
            return os.getuid() == 0
        else:
            raise RuntimeError("Unsupported operating system for this module: %s" % (os.name,))

    @staticmethod
    def runAsAdmin(cmdLine=None, wait=True):

        if os.name != 'nt':
            raise RuntimeError("This function is only implemented on Windows.")

        import win32api, win32con, win32event, win32process
        from win32com.shell.shell import ShellExecuteEx
        from win32com.shell import shellcon

        python_exe = sys.executable

        if cmdLine is None:
            cmdLine = [python_exe] + sys.argv
        elif type(cmdLine) not in (tuple, list):
            raise ValueError("cmdLine is not a sequence.")
        cmd = '"%s"' % (cmdLine[0],)
        # XXX TODO: isn't there a function or something we can call to massage command line params?
        params = " ".join(['"%s"' % (x,) for x in cmdLine[1:]])
        cmdDir = ''
        showCmd = win32con.SW_SHOWNORMAL
        #showCmd = win32con.SW_HIDE
        lpVerb = 'runas'  # causes UAC elevation prompt.

        # print "Running", cmd, params

        print(f"cmdLine: {cmdLine}")

        # ShellExecute() doesn't seem to allow us to fetch the PID or handle
        # of the process, so we can't get anything useful from it. Therefore
        # the more complex ShellExecuteEx() must be used.

        # procHandle = win32api.ShellExecute(0, lpVerb, cmd, params, cmdDir, showCmd)

        procInfo = ShellExecuteEx(nShow=showCmd,
                                  fMask=shellcon.SEE_MASK_NOCLOSEPROCESS,
                                  lpVerb=lpVerb,
                                  lpFile=cmd,
                                  lpParameters=params)

        if wait:
            procHandle = procInfo['hProcess']
            obj = win32event.WaitForSingleObject(procHandle, win32event.INFINITE)
            rc = win32process.GetExitCodeProcess(procHandle)
            #print "Process handle %s returned code %s" % (procHandle, rc)
        else:
            rc = None

        return rc

    @staticmethod
    def start_as_admin(func=None):
        rc = 0
        if not Admin_Handler.isUserAdmin():
            print("You're not an admin.", os.getpid(), "params: ", sys.argv)
            #rc = runAsAdmin(["c:\\Windows\\notepad.exe"])
            rc = Admin_Handler.runAsAdmin()
        else:
            print("You are an admin!", os.getpid(), "params: ", sys.argv)
            rc = 0

            if func: fn = func()

        x = input('Press Enter to exit.')
        return rc


if __name__ == "__main__":
    sys.exit(Admin_Handler.start_as_admin(lambda : print()))
