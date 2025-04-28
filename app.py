# app.py
import logging
import os
from flask import Flask, jsonify, g
from flask_cors import CORS

from config import config
from utils.log import (
    setup_logging,
    log_api_request,
)  # Import centralized logging setup and handler

# Setup logging *before* creating the app or blueprints
setup_logging()
logger = logging.getLogger(__name__)  # Get logger for this module


def create_app():
    """Creates and configures the Flask application."""
    app = Flask(__name__)
    app.config.from_object(config)  # Load config from config.py

    # Enable CORS for all origins (adjust in production if needed)
    CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

    # Register request logging hooks from utils.log
    @app.before_request
    def before_request_hook():
        """Sets up g context before each request."""
        from time import perf_counter
        from datetime import datetime, timezone

        g.start_time = perf_counter()
        g.request_time = datetime.now(timezone.utc)
        g.username = None  # Default, can be set by endpoint
        g.log_outcome = "unknown"
        g.log_error_message = None

    @app.after_request
    def after_request_hook(response):
        """Logs request details using the centralized handler."""
        return log_api_request(response)

    # Import and register Blueprints for API endpoints
    try:
        from api.auth import auth_bp
        from api.guc import guc_bp
        from api.schedule import schedule_bp
        from api.cms import cms_bp
        from api.grades import grades_bp
        from api.attendance import attendance_bp
        from api.exams import exams_bp
        from api.proxy import proxy_bp
        from api.admin import admin_bp
        from api.misc import misc_bp
        from api.notifications import notifications_bp

        app.register_blueprint(auth_bp, url_prefix="/api")
        app.register_blueprint(guc_bp, url_prefix="/api")
        app.register_blueprint(schedule_bp, url_prefix="/api")
        app.register_blueprint(cms_bp, url_prefix="/api")
        app.register_blueprint(grades_bp, url_prefix="/api")
        app.register_blueprint(attendance_bp, url_prefix="/api")
        app.register_blueprint(exams_bp, url_prefix="/api")
        app.register_blueprint(proxy_bp, url_prefix="/api")  # Proxy/Extract endpoints
        app.register_blueprint(
            admin_bp, url_prefix="/api"
        )  # Admin endpoints under /api/admin
        app.register_blueprint(misc_bp, url_prefix="/api")  # Other endpoints
        app.register_blueprint(
            notifications_bp, url_prefix="/api"
        )  # Notifications endpoint

        logger.info("Registered API Blueprints")
    except ImportError as e:
        logger.critical(f"Failed to import or register Blueprints: {e}", exc_info=True)
        # Decide if the app should fail to start or run without blueprints
        # raise e # Raise error to prevent startup

    # Basic root route
    @app.route("/")
    def index():
        logger.info("Root endpoint '/' accessed.")
        # Avoid logging this via after_request hook if desired
        # Returning simple JSON, not triggering the standard log format intentionally here
        # as it's just a health check / welcome message.
        return jsonify({"message": "Unisight API is running."}), 200

    # Catch-all route for undefined paths under /api/ (optional)
    @app.route("/api/<path:invalid_path>")
    def invalid_api_route(invalid_path):
        logger.warning(f"Invalid API path accessed: /api/{invalid_path}")
        g.log_outcome = "not_found"
        g.log_error_message = f"Invalid API endpoint: /api/{invalid_path}"
        return jsonify({"status": "error", "message": "Invalid API endpoint"}), 404

    logger.info("Flask app created successfully.")
    return app


# Create the app instance for Vercel or WSGI server
app = create_app()

# This block is for running locally with `python app.py`
# Vercel deployment will use the 'app' variable directly.
if __name__ == "__main__":
    # Use host='0.0.0.0' to be accessible externally if needed
    # Port 5000 is common for Flask dev server
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting Flask development server on port {port}")
    # debug=config.DEBUG will enable Flask's debugger and reloader
    app.run(host="0.0.0.0", port=port, debug=config.DEBUG)
