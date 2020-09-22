from flask import Flask

app = Flask(__name__)

from server.app import routes
app.run()