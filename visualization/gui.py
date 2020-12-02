import tkinter, sys


class Gui(tkinter.Frame):
    def __init__(self, master=None):  # gets called first
        super().__init__(master)
        self.master = master  # base object
        self.quit = tkinter.Button(self, text="QUIT", fg="red", command=self.master.destroy)
        self.pack()
        self.create_widgets()

    def create_widgets(self):

        # QUIT
        self.quit.pack(side="bottom")

    def exit(self):  # maybe
        self.master.destroy
        sys.exit(0)
