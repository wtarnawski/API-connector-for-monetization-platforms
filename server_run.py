from flask import Flask

app = Flask(__name__)


@app.route('/')
@app.route('/index')
def index():
    from merge_data_sources_and_push_to_spreadsheets import run
    run()
    return "OK"


app.run()