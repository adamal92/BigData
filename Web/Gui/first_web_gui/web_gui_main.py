from flask import Flask, render_template
from flaskwebgui import FlaskUI   # get the FlaskUI class


app = Flask(__name__)

# Feed it the flask app instance
ui = FlaskUI(app)                 # feed the parameters


# do your logic as usual in Flask

@app.route("/")
def index():
    # return "It works!"
    return render_template('index.html')


@app.route("/home", methods=['GET'])
def home():
    return render_template('home.html')
    # return "ok"


if __name__ == '__main__':
    ui.run()                           # call the 'run' method
