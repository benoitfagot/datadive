import time, sys, os
from app import create_app

def run_server(app):
    app.run(host="0.0.0.0", port=5000)

if __name__ == "__main__":
    app = create_app()
    run_server(app)