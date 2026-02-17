from src import app
from flask import jsonify


# Health endpoint with JSON response for Tusk Drift setup. Remove this endpoint if not needed (and update .tusk/config.yaml accordingly).
@app.route('/health', endpoint='health_check', methods=['GET'])
def health_check():
    """Simple health check endpoint that returns JSON"""
    return jsonify({"status": "ok"})
