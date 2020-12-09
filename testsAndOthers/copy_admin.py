import os

admin_handler_path: str = ""
for directory in os.getcwd().split("\\")[:-1]:
    admin_handler_path += f"{directory}\\"
print(admin_handler_path, os.getcwd(), os.getcwd().split("\\")[:-1])
admin_handler_path += r"testsAndOthers\administrator_handler.py"

with open(admin_handler_path, "r") as admin_file:
    with open(__file__, "a") as exec_file:
        exec_file.write(admin_file.read())
        exec_file.write('if __name__ == \'__main__\':\n\tsys.exit(Admin_Handler.start_as_admin(admin_func)')
    with open(__file__, "r") as exec_file:
        print(exec_file.read())
