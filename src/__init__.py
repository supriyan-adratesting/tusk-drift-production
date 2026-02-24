from flask import Flask

app = Flask(__name__)

# Importing controllers
from src.controllers.authentication.manual import authentication_controller
from src.controllers.authentication.linkedin import linkedin_controller
from src.controllers.authentication.apple import apple_controller
from src.controllers.authentication.google import google_controller
from src.controllers.professional import professional_controller
from src.controllers.partner import partner_controller
from src.controllers.employer import employer_controller
from src.controllers.payment import payment_controller
from src.controllers.admin import admin_controller
# Lazy load chat_bot_controller to avoid Qdrant initialization issues during replay
# from src.controllers.chat_bot import chat_bot_controller
from src.controllers.health import health_controller

# Lazy-load chatbot controller on first request to avoid initialization issues in test/replay mode
@app.before_request
def _lazy_load_chatbot():
    # Only import once
    if not hasattr(_lazy_load_chatbot, 'loaded'):
        try:
            from src.controllers.chat_bot import chat_bot_controller
            _lazy_load_chatbot.loaded = True
        except Exception as e:
            # In replay mode, chatbot might fail to initialize - that's ok for health checks
            import os
            if os.environ.get('TUSK_DRIFT_MODE') == 'REPLAY':
                _lazy_load_chatbot.loaded = True  # Mark as loaded to avoid retrying
            else:
                raise



