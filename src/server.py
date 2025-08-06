from flask import Flask, request

"""This script runs a flask server listening to the local host on port 8182 to receive callback url and return authentication code"""

codes = []

app = Flask(__name__)

@app.route("/")
def page() ->str:
    code = request.args.get("code")
    codes.append(code)
    return ''

def run_server()-> None:
    """ Runs HTTPS server with self-signed certificate"""
    app.run(ssl_context = ('cert.pem','key.pem'), port=8182, debug= False)
