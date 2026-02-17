from flask import Flask, request, redirect
from src import app
from src.controllers.payment import payment_process as payment
from src.controllers.jwt_tokens import jwt_token_required as jwt_token

from src.controllers.payment.payment_process_new import Payment

new_payment = Payment()

@app.route('/create_checkout_session',endpoint='create_checkout_session',methods=['POST'])
@jwt_token.token_required
def checkoutSession():
    result = payment.create_checkout_session(request)
    return result

@app.route('/create_new_checkout_session',endpoint='create_new_checkout_session',methods=['POST'])
@jwt_token.token_required
def create_new_checkout_session():
    result = new_payment.create_checkout_session(request)
    return result

@app.route('/update_checkout_status', methods=['POST'])
def update_checkout_status():
    result = payment.update_checkout_status(request)
    return result

@app.route("/get_checkout_status", methods=['POST'])
def getCheckout():
    result = payment.get_checkout_status(request)
    return result

@app.route("/get_new_checkout_status", methods=['POST'])
def get_new_checkout_status():
    result = new_payment.get_checkout_status(request)
    return result

@app.route("/cancel_subscription", methods=['GET'])
def cancelSubscription():
    result = payment.cancel_subscription(request)
    return result

@app.route("/new_cancel_subscription", methods=['GET', 'POST'])
def new_cancel_subscription():
    result = new_payment.cancel_subscription(request)
    return result

@app.route("/switch_plan",endpoint='switch_plan',methods=['POST'])
@jwt_token.token_required
def switchplan():
    result = payment.switch_plan(request)
    return result

@app.route("/upgrade_plan",endpoint='upgrade_plan', methods=['POST'])
@jwt_token.token_required
def upgrade_plan():
    result = payment.upgrade_subscription(request)
    return result

@app.route('/update_checkout_status_partner', methods=['POST'])
def updateCheckout():
    result = payment.updateCheckoutStatusPartner(request)
    return result

@app.route('/webhook', methods=['POST'])
def webhook():
    result = payment.dev_webhook(request)
    return result

# New Stripe webhook
@app.route('/stripe_new_webhook', methods=['POST'])
def stripe_new_webhook():
    result =new_payment.stripe_webhook_endpoint(request)
    return result

# New Razorpay webhook
@app.route('/razorpay_new_webhook', methods=['POST'])
def razorpay_new_webhook():
    result = new_payment.razorpay_webhook_endpoint(request)
    return result

@app.route('/create_payment_session',endpoint='create_payment_session',methods=['POST'])
@jwt_token.token_required
def create_payment_session():
    result = payment.create_payment_session(request)
    return result

@app.route('/razorpay_create_checkout_session',endpoint='razorpay_create_checkout_session',methods=['POST'])
@jwt_token.token_required
def razorpay_create_checkout_session():
    result = payment.razorpay_create_checkout_session(request)
    return result

# Razorpay trial subscription
@app.route("/create_razorpay_trial",endpoint='create_razorpay_trial', methods=['POST'])
@jwt_token.token_required
def create_razorpay_trial():
    result = new_payment.razorpay_create_trial(request)
    return result

# Stripe trial subscription
@app.route("/create_stripe_trial",endpoint='create_stripe_trial', methods=['POST'])
@jwt_token.token_required
def create_stripe_trial():
    result = new_payment.stripe_create_trial(request)
    return result

@app.route("/razorpay_upgrade_plan",endpoint='razorpay_upgrade_plan', methods=['POST'])
@jwt_token.token_required
def razorpay_upgrade_plan():
    result = payment.razorpay_upgrade_plan(request)
    return result

@app.route("/razorpay_switch_plan",endpoint='razorpay_switch_plan', methods=['POST'])
@jwt_token.token_required
def razorpay_switch_plan():
    result = payment.razorpay_switch_plan(request)
    return result

@app.route('/razorpay_create_payment_session',endpoint='razorpay_create_payment_session',methods=['POST'])
@jwt_token.token_required
def razorpay_create_payment_session():
    result = payment.razorpay_create_payment_session(request)
    return result

@app.route('/razorpay_webhook', methods=['POST'])
def razorpay_webhook():
    result = payment.razorpay_webhook()
    return result

@app.route('/cancel_razorpay_subscription', methods=['POST'])   #razorpay
def cancel_razorpay_subscription():
    result = payment.cancel_razorpay_subscription()
    return result

@app.route('/create_order', methods=['POST'])
def create_order():
    result = payment.create_order(request)
    return result

@app.route('/get_checkout_user', methods=['GET'])
# @jwt_token.token_required
def get_checkout_user():
    result = new_payment.get_user_checkout_details(request)
    return result

# @app.route('/capture_payment', methods=['POST'])   #razorpay
# def capture_payment():
#     result = payment.capture_payment()
#     return result

# @app.route('/chargebee_checkout', methods=['GET'])
# def chargebee_checkout():
#     result = payment.chargebee_checkout()
#     return result

# @app.route('/cb_webhook', methods=['POST'])
# def cb_webhook():
#     result = payment.cb_webhook()
#     return result