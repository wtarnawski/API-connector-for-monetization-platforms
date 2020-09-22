from server.app import app
@app.route('/')
@app.route('/index')
def index():
    from merge_data_sources_and_push_to_spreadsheets import run
    run()
    return "OK"