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
from src.controllers.chat_bot import chat_bot_controller
from src.controllers.health import health_controller



