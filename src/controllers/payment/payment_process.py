from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from flask import jsonify, request, redirect
import json
import stripe
import os
from datetime import datetime, timezone
from base64 import b64encode,b64decode
from geopy.geocoders import Nominatim
from src.controllers.jwt_tokens.jwt_token_required import token_authentication,get_user_token
from src.models.user_authentication import api_json_response_format, get_user_data, get_sub_user_data

from datetime import timedelta
from datetime import datetime as dt
import time
import platform
from flask_executor import Executor
from src.models.background_task import BackgroundTask

# import chargebee

executor = Executor(app)
background_runner = BackgroundTask(executor)

API_URI = os.environ.get('API_URI')
STRIPE_KEY = os.environ.get('STRIPE_KEY')
WH_SEC_KEY = os.environ.get('WH_SEC_KEY')
BASIC_PLAN_ID = os.environ.get('BASIC_PLAN_ID')
# PREMIUM_PLAN_ID = os.environ.get('PREMIUM_PLAN_ID')
ELITE_PLAN_ID = os.environ.get('ELITE_PLAN_ID')
EMPLOYER_BASIC = os.environ.get('EMPLOYER_BASIC')
EMPLOYER_PREMIUM = os.environ.get('EMPLOYER_PREMIUM')
EMPLOYER_PLATINUM = os.environ.get('EMPLOYER_PLATINUM')
# PARTNER_BASIC = os.environ.get('PARTNER_BASIC')
# PARTNER_PREMIUM = os.environ.get('PARTNER_PREMIUM')
# PARTNER_PLATINUM = os.environ.get('PARTNER_PLATINUM')
ADD_JOB = os.environ.get('ADDITIONAL_JOB')
EXTEND_JOB = os.environ.get('EXTEND_JOB')
ADD_POST = os.environ.get('ADDITIONAL_ADS')
EXTEND_POST = os.environ.get('EXTEND_ADS')
WEB_APP_URI = os.environ.get('WEB_APP_URI')



stripe.api_key = STRIPE_KEY


def create_checkout_session(request):
    try:
        print("check out session")
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]
            user_data = get_user_data(user_email_id)
            user_id = user_data['user_id']
        request_json = request.get_json()
        attribute_name = request_json['attribute_name']

        # pricing_currency_arr = str(attribute_name).split('_')
        # pricing_currency = pricing_currency_arr[3]
        # insert_pricing_currency_query = 'update users set payment_currency = %s where email_id = %s'
        # user_values = (pricing_currency, user_email_id,)
        # update_query(insert_pricing_currency_query, user_values)

        # insert_currency_sub_users_query = 'update sub_users set payment_currency = %s where user_id = %s'
        # sub_user_values = (pricing_currency, user_id,)
        # update_query(insert_currency_sub_users_query, sub_user_values)

        pricing_key, user_role, trial_period, plan_name = '', '', '', ''

        get_plan_details_query = "select attribute_value, role, plan_name from payment_config where attribute_name = %s"
        values = (attribute_name,)
        plan_details = execute_query(get_plan_details_query, values)
        if len(plan_details) > 0:
            pricing_key = plan_details[0]['attribute_value']
            user_role = plan_details[0]['role']
            plan_name = plan_details[0]['plan_name']
        else:
            return api_json_response_format("False", "Invalid attribute name", 500, {})
        get_trial_period_query = "select attribute_value from payment_config where attribute_name = %s"

        if user_role == 'Employer':
            values = ('employer_trial_period',)
        elif user_role == 'Partner':
            values = ('partner_trial_period',)
        else:
            values = ('professional_trial_period',)

        trial_period_dict = execute_query(get_trial_period_query, values)
        if len(trial_period_dict) > 0:
            trial_period = trial_period_dict[0]['attribute_value']

        existing_customers = stripe.Customer.list(email=user_email_id)['data']
        if not existing_customers:
            customer = stripe.Customer.create(
                email=user_email_id,
                metadata={'user_id': user_email_id},  # Storing user ID in metadata for reference
            )
            customer_id = customer['id']

            insert_customer_query = "INSERT INTO stripe_customers (email, customer_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE customer_id = %s;"
            insert_customer_values = (user_email_id, customer_id, customer_id,)
            update_query(insert_customer_query, insert_customer_values)
        else:
            # If the customer exists, get the customer ID
            customer_id = existing_customers[0]['id']
        print(f"From trial {customer_id}")

        if user_role == 'Professional' : 
            checkout_session = stripe.checkout.Session.create(line_items=[
                                    {
                                    'price': pricing_key,
                                    'quantity': 1,
                                    },
                                    ],
                                    customer=customer_id,
                                    allow_promotion_codes = True,
                                    mode = 'subscription',
                                    subscription_data = {
                                        "metadata": { "user_id" : user_email_id, "plan" : plan_name, "type" : "subscription", "existing_plan" : "default", "new_user" : "yes"},
                                        "trial_settings": {"end_behavior": {"missing_payment_method": "cancel"}},
                                        "trial_period_days" : int(trial_period)
                                        },
                                    payment_method_collection="if_required",
                                    # discounts=[{
                                    # 'coupon': 'promo_1NGL78DW6EemFUWry3pS8Axs', ##sholud be created in stripe dashboard
                                    # }],
                                    success_url = WEB_APP_URI +
                                    '/checkout_success?flag=success&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,
                                    cancel_url = WEB_APP_URI + '/checkout_failure?flag=failure&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,)
        else:
            if user_role == 'Employer':
                success_url = WEB_APP_URI +'/checkout_success?flag=success&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id
                cancel_url = WEB_APP_URI + '/checkout_failure?flag=failure&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id
            elif user_role == 'Partner':
                success_url = WEB_APP_URI +'/checkout_success?flag=success&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id
                cancel_url = WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id
            checkout_session = stripe.checkout.Session.create(line_items=[
                                        {
                                        'price': pricing_key,
                                        'quantity': 1,
                                        },
                                        ],
                                        customer=customer_id,
                                        allow_promotion_codes = True,
                                        mode = 'subscription',
                                        subscription_data = {
                                            "metadata": { "user_id" : user_email_id, "plan" : plan_name, "type" : "subscription", "existing_plan" : "default", "from_trial" : "yes", "new_user" : "yes"},
                                            "trial_settings": {"end_behavior": {"missing_payment_method": "cancel"}},
                                            "trial_period_days" : int(trial_period)
                                            },
                                        payment_method_collection = "if_required",
                                        success_url = success_url,
                                        cancel_url = cancel_url,)
        # print(f"checkout session : {checkout_session}")
        # return redirect(checkout_session.url, code = 303)
        return api_json_response_format(True,"success",0,{"url":checkout_session.url})
    except Exception as e:
        print("Error in checkout_session:", e)

def update_checkout_status(request):
    try:
        request_json = request.get_json()
        session_id = request_json['session_id']
        user_id = request_json['user_id']
        flag = request_json['flag']
        query = "select user_role_fk from users where email_id = %s"
        values = (user_id,)
        role_obj = execute_query(query,values)
        if not role_obj:
            query = "select role_id as user_role_fk from sub_users where email_id = %s"
            values = (user_id,)
            role_obj = execute_query(query,values)
        role_id = 0
        if len(role_obj) > 0:
            role_id = role_obj[0]["user_role_fk"]
        print(f"session_id {session_id}, user_id {user_id}, flag {flag}")
        if flag == 'success':            
            # session = stripe.checkout.Session.retrieve(session_id,)
            # customer = stripe.Customer.retrieve(session.customer)
            # print("Customer_name : " + str(customer.name))
            # email_id = b64decode(user_id.encode("utf-8")).decode("utf-8")
            # if flag == "success":                
            return api_json_response_format(True, f"Thank you {user_id} for subscribing with us.",0,{"user_role":role_id}) 
            # else:
            #     return api_json_response_format(True, f"Sorry {user_id} but your checkout process has failed. Please try again.",0,{"user_role":role_id})
        else:
            return api_json_response_format(True, f"Sorry {user_id} but your checkout process has failed. Please try again.",0,{"user_role":role_id})
    except Exception as e:
        print("Error in checkout_session:", e)

def switch_plan(request):
    try:
        print("switch_plan")
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]
        user_data = get_user_data(user_email_id)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(user_email_id)
            if user_data["is_exist"]:
                query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                values = (user_email_id,)
                email_id_dict = execute_query(query, values)
                if email_id_dict:
                    user_email_id = email_id_dict[0]['email_id']
        request_json = request.get_json()
        attribute_name = request_json['attribute_name']

        # pricing_currency_arr = str(attribute_name).split('_')
        # pricing_currency = pricing_currency_arr[3]
        # print(pricing_currency)

        get_existing_plan_query = "SELECT pricing_category, payment_status, subscription_id, existing_pricing_key, current_period_start, current_period_end, current_sub_item_id, payment_currency FROM users WHERE email_id = %s UNION SELECT pricing_category, payment_status, subscription_id, existing_pricing_key, current_period_start, current_period_end, current_sub_item_id, payment_currency FROM sub_users WHERE email_id = %s;" 
        get_existing_plan_values = (user_email_id, user_email_id,)
        existing_pricing_category_dict = execute_query(get_existing_plan_query, get_existing_plan_values)
        payment_status = None
        current_subscription_id = ''
        current_sub_item_id = ''
        current_period_start = 0
        current_period_end = 0
        pricing_currency = ''
        if existing_pricing_category_dict:
            payment_status = existing_pricing_category_dict[0]['payment_status']
            pricing_currency = existing_pricing_category_dict[0]['payment_currency']
            if payment_status == 'active':
                current_subscription_id = existing_pricing_category_dict[0]['subscription_id']
            existing_pricing_category = str(existing_pricing_category_dict[0]['pricing_category'])
            existing_pricing_key = existing_pricing_category_dict[0]['existing_pricing_key']
            current_period_start = existing_pricing_category_dict[0]['current_period_start']
            current_period_end = existing_pricing_category_dict[0]['current_period_end']
            current_sub_item_id = existing_pricing_category_dict[0]['current_sub_item_id']
        else:
            existing_pricing_category = "default"
            existing_pricing_key = ''
        get_plan_details_query = "SELECT attribute_value, role, plan_name FROM payment_config WHERE attribute_name = %s"
        values = (attribute_name,)
        plan_details = execute_query(get_plan_details_query, values)
        
        if len(plan_details) > 0:
            pricing_key = plan_details[0]['attribute_value']
            plan = plan_details[0]['plan_name']
            print(f"plan details from switch plan {plan_details}")
        else:
            return api_json_response_format("False", "Invalid attribute name", 500, {})

        query = "SELECT customer_id FROM stripe_customers WHERE email = %s;"
        values = (user_email_id,)
        email_id_dict = execute_query(query, values)
        customer_id = ''
        if email_id_dict:
            customer_id = email_id_dict[0]['customer_id']
            print(customer_id)
        else:
            return api_json_response_format("False", "No customer found with the provided email", 404, {})
        price = stripe.Price.retrieve(pricing_key)
        if price:
            product_id = price['product']
        else:
            print("Error in retrieving product id")
            product_id = ''

        current_plan_amount = stripe.Price.retrieve(existing_pricing_key)['unit_amount']
        new_plan_amount = stripe.Price.retrieve(pricing_key)['unit_amount']
        print(f"current plan amount --> {current_plan_amount} ::: new plan amount --> {new_plan_amount}")
        is_upgrade = new_plan_amount > current_plan_amount
        
        if is_upgrade:
            billing_cycle_end = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            billing_cycle_start = datetime.fromtimestamp(current_period_start, tz=timezone.utc)
            total_days = (billing_cycle_end - billing_cycle_start).days
            days_used = (datetime.now(timezone.utc) - billing_cycle_start).days
            # days_used = 190
            daily_cost_current_plan = round(current_plan_amount / total_days, 2)
            # billing_cycle_end = datetime.fromtimestamp(current_period_end)
            # billing_cycle_start = datetime.fromtimestamp(current_period_start)
            # total_days = (billing_cycle_end - billing_cycle_start).days
            # days_used = (datetime.now() - billing_cycle_start).days
            # remaining_days = total_days - days_used
            # credit = daily_cost_current_plan * remaining_days
            # daily_cost_current_plan = current_plan_amount / total_days
            amount_used = daily_cost_current_plan * days_used
            amount_left = current_plan_amount - amount_used
            amount_to_pay = new_plan_amount - amount_left
            new_price = stripe.Price.create(
                    unit_amount = int(amount_to_pay),
                    currency = pricing_currency,
                    recurring = {"interval": "year"},  # Replace with the correct interval
                    product = product_id, 
                    nickname = f"Adjusted Plan for Upgrade"
                )
            if new_price:
                new_price_id = new_price['id']
            else:
                message = "Unable to create a new price in Stripe."
            # Update the subscription with the new price
            checkout_session = stripe.checkout.Session.create(line_items=[
                                {
                                'price': new_price_id,
                                'quantity': 1,
                                },
                                ],
                                customer=customer_id,
                                allow_promotion_codes = True,
                                mode = 'subscription',
                                payment_method_collection='always',
                                subscription_data = {
                                    "metadata": { "user_id" : user_email_id, "plan" : plan, "existing_plan" : existing_pricing_category, "type" : "subscription", "existing_pricing_key" : pricing_key,
                                                    "existing_subscription_id": current_subscription_id, "adjusted_price" : "Yes", "from_upgrade" : "Yes"} #, "is_canceled" : 'no'
                                    },
                                success_url=WEB_APP_URI +
                                '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,
                                cancel_url=WEB_APP_URI + '/checkout_failure?flag=failure&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,)
            message = "Success"
            print(checkout_session.url)
            data = {"url" : checkout_session.url}
        else:
            current_period_start = datetime.fromtimestamp(current_period_start, tz=timezone.utc)
            current_period_end = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            total_days = (current_period_end - current_period_start).days
            days_used = (datetime.now(timezone.utc) - current_period_start).days
            remaining_days = total_days - days_used

            daily_cost_current_plan = current_plan_amount / total_days
            daily_cost_new_plan = new_plan_amount / total_days
            credit = daily_cost_current_plan * remaining_days
            charge = daily_cost_new_plan * remaining_days
            # adjustment_amount = charge - credit
            adjustment_amount = credit - charge

            current_subscription = stripe.Subscription.retrieve(current_subscription_id)

            if adjustment_amount > 0:
                stripe.InvoiceItem.create(
                    customer = customer_id,
                    amount=int(adjustment_amount),
                    currency = pricing_currency,
                    description="Adjustment for plan downgrade"
                )
                stripe.Invoice.create(customer = customer_id)
            elif adjustment_amount < 0:
                stripe.CreditNote.create(
                    invoice=current_subscription['latest_invoice'],
                    amount=int(abs(adjustment_amount))
                )
            
            checkout = stripe.Subscription.modify(
                    current_subscription_id,
                    cancel_at_period_end=True,
                    items=[{
                        "id": current_subscription['items']['data'][0]['id'],
                        "price": pricing_key,
                    }],
                    metadata={
                        "plan": plan,
                        "type": "subscription",
                        "existing_plan": existing_pricing_category,
                        "new_user" : "no", 
                        "existing_pricing_key" : pricing_key,
                        "from_downgrade": "yes"
                    }
                )

            # stripe.Subscription.modify(
            #     current_subscription_id,
            #     cancel_at_period_end = True,
            #     items=[{
            #         "id": current_sub_item_id,
            #         "price": pricing_key,
            #     }],
            #     metadata = {"plan": plan, "type" : "subscription", "existing_plan" : existing_pricing_category, "new_user" : "no", "existing_pricing_key" : pricing_key, "from_downgrade" : "yes"},
            #     # billing_cycle_anchor = "unchanged",
            #     proration_behavior = "none"
            # )
            message = "Success"
            data = {"url" : checkout}
            # subscription = stripe.Subscription.retrieve(current_subscription_id)
            # sub_end_date = subscription.current_period_end
            # stripe.Subscription.modify(
            #     subscription.id,
            #     cancel_at_period_end=True 
            #     )
            # print(subscription)
            # default_payment_id = subscription.default_payment_method
            # print(default_payment_id)
            # schedule = stripe.SubscriptionSchedule.create(
            #     customer=subscription.customer,
            #     start_date=sub_end_date, # Current date
            #     end_behavior='release',
            #     default_settings={
            #         'collection_method': 'charge_automatically',
            #         },
            #     phases=[
            #         {
            #         'items': [{
            #                     'price': subscription.plan.id,
            #                 }],
            #         "iterations": 1,
            #         "metadata" : {"user_id" : user_email_id, "plan": plan, "type" : "subscription", "existing_plan" : existing_pricing_category, "new_user" : "no", "from_downgrade" : "yes"}
            #         },
            #         {
            #         'items': [{
            #         'price': pricing_key,
            #         }],
            #         'default_payment_method' : default_payment_id,
            #         "metadata" : {"user_id" : user_email_id, "plan": plan, "type" : "subscription", "existing_plan" : existing_pricing_category, "new_user" : "no", "from_downgrade" : "yes"}
            #         },
            #     ],
            # )
            # print(schedule)

        return api_json_response_format("True", message, 200, data)
    
    except Exception as e:
        print("Error in switch_plan:", e)
        return api_json_response_format("False", "An error occurred in switch_plan()", 500, {})

# def upgrade_plan():
#     try:
#         print("upgrade_plan")
#         token_result = get_user_token(request)                          
#         if token_result["status_code"] == 200:  
#             user_email_id = token_result["email_id"]
#         request_json = request.get_json()
#         attribute_name = request_json['attribute_name']

#         get_plan_details_query = "SELECT attribute_value, plan_name FROM payment_config WHERE attribute_name = %s"
#         values = (attribute_name,)
#         plan_details = execute_query(get_plan_details_query, values)
        
#         if not plan_details:
#             return api_json_response_format("False", "Invalid attribute name", 500, {})

#         pricing_key = plan_details[0]['attribute_value']
#         plan_name = plan_details[0]['plan_name']
#         subscription = stripe.Subscription.list(customer_email=user_email_id, status='active')['data']
#         if not subscription:
#             return api_json_response_format("False", "No active subscription found", 404, {})
        
#         current_subscription = subscription[0]

#         updated_subscription = stripe.Subscription.modify(
#             current_subscription['id'],
#             items=[{
#                 "id": current_subscription['items']['data'][0]['id'],
#                 "price": pricing_key,
#             }],
#             proration_behavior="create_prorations",
#         )

#         upcoming_invoice = stripe.Invoice.upcoming(customer=current_subscription['customer'])
#         proration_details = [
#             item for item in upcoming_invoice['lines']['data'] if item['type'] == 'subscription'
#         ]

#         print("Proration Details:", proration_details)

#         return api_json_response_format("True", "Plan upgraded successfully", 200, {
#             "proration_details": proration_details,
#             "new_plan": plan_name,
#         })

#     except Exception as e:
#         print("Error in upgrade_plan:", e)
#         return api_json_response_format("False", "An error occurred", 500, {})

def upgrade_subscription(request):
    try:
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            email_id = token_result["email_id"]
        user_data = get_user_data(email_id)
        user_id = user_data['user_id']
        if not user_data['is_exist']:
            user_data = get_sub_user_data(email_id)
            if user_data["is_exist"]:
                query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                values = (email_id,)
                email_id_dict = execute_query(query, values)
                if email_id_dict:
                    email_id = email_id_dict[0]['email_id']
        query = "SELECT count(id) FROM stripe_customers WHERE email = %s;"
        values = (email_id,)
        email_id_dict = execute_query(query, values)
        customer_id = ''
        if email_id_dict:
            if email_id_dict[0]['count(id)'] == 0:
                customer = stripe.Customer.create(
                    email = email_id,
                    metadata = {"user_id": email_id},  # Storing user ID in metadata for reference
                    )
                customer_id = customer['id']

                insert_customer_query = "INSERT INTO stripe_customers (email, customer_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE customer_id = %s;"
                insert_customer_values = (email_id, customer_id, customer_id,)
                update_query(insert_customer_query, insert_customer_values)
            else:
                query = "SELECT customer_id FROM stripe_customers WHERE email = %s;"
                values = (email_id,)
                email_id_dict_1=  execute_query(query, values)
                customer_id = ''
                if email_id_dict_1:
                    customer_id = email_id_dict_1[0]['customer_id']
                else:
                    customers = stripe.Customer.list(email=email_id)
                    for customer in customers.data:
                        if customer.email == email_id:
                            customer_id =  customer.id
            
        request_data = request.get_json()
        get_existing_plan_query = "SELECT pricing_category, subscription_id, payment_status FROM users WHERE email_id = %s;" 
        get_existing_plan_values = (email_id,)
        existing_pricing_category_dict = execute_query(get_existing_plan_query, get_existing_plan_values)
        payment_status = None
        trial_subscription_id = ''
        if existing_pricing_category_dict:
            existing_pricing_category = existing_pricing_category_dict[0]['pricing_category']
            payment_status = existing_pricing_category_dict[0]['payment_status']
            if payment_status == 'trialing':
                trial_subscription_id = existing_pricing_category_dict[0]['subscription_id']
                stripe.Subscription.delete(trial_subscription_id)
                time.sleep(5)
        else:
            existing_pricing_category = 'default'
        attribute_name = request_data['attribute_name']

        # pricing_currency_arr = str(attribute_name).split('_')
        # pricing_currency = pricing_currency_arr[3]
        # insert_pricing_currency_query = 'update users set payment_currency = %s where email_id = %s'
        # user_values = (pricing_currency, email_id,)
        # update_query(insert_pricing_currency_query, user_values)

        # insert_currency_sub_users_query = 'update sub_users set payment_currency = %s where user_id = %s'
        # sub_user_values = (pricing_currency, user_id,)
        # update_query(insert_currency_sub_users_query, sub_user_values)

        get_plan_details_query = "SELECT attribute_value, role, plan_name FROM payment_config WHERE attribute_name = %s"
        values = (attribute_name,)
        plan_details = execute_query(get_plan_details_query, values)
        new_pricing_key = ''       #'price_1PnHTXDW6EemFUWrBn5IVSzG'
        plan = ''
        if len(plan_details) > 0:
            new_pricing_key = plan_details[0]['attribute_value']
            plan = plan_details[0]['plan_name']
        else:
            return api_json_response_format("False", "Invalid attribute name", 500, {})

        checkout_session = stripe.checkout.Session.create(
            line_items=[{
                'price': new_pricing_key,
                'quantity': 1,
            }],
            customer=customer_id,
            allow_promotion_codes=True,
            mode='subscription',
            payment_method_collection="always",
            subscription_data={
                "metadata": {
                    "user_id": email_id,
                    "plan": plan,
                    "type" : 'subscription',
                    "existing_plan" : existing_pricing_category,
                    "existing_pricing_key" : new_pricing_key,
                    "existing_subscription_id" : trial_subscription_id,
                    "from_upgrade" : "Yes"
                }
            },
            success_url = WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+email_id,
            cancel_url = WEB_APP_URI + '/checkout_failure?flag=failure&session_id={CHECKOUT_SESSION_ID}&user_id='+email_id
            )
        print(f"Subscription upgraded: {checkout_session}")
        print(checkout_session.url)
        return api_json_response_format(True,"success",0,{"url":checkout_session.url})
    except Exception as e:
        return f"Error upgrading subscription: {str(e)}"

def dev_webhook(request):
    try:
        signature = request.headers.get('stripe-signature')
        try:
            event = stripe.Webhook.construct_event(
            payload = request.data, sig_header=signature, secret=WH_SEC_KEY)
            data = event['data']
        except Exception as e:
            print("Webhook Error : "+str(e))
            return e
        event_type = event['type'] 
        print("Event Type::: " + str(event_type))
        data_object = data['object']
        checkout_status_msg = ""

        if event_type == 'checkout.session.completed':
            try:
                email_id = data_object['customer_details']['email'] #discount = json_data.get("data", {}).get("object", {}).get("discount", {})
                metadata = data_object.get('metadata', {})
                type = metadata.get('type','')
                checkout_status = data_object['status']
                print(f"Checkout status from session complete: {checkout_status}")
                user_details_query = "SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;"
                user_details_values = (email_id,)
                user_detail = execute_query(user_details_query, user_details_values)
                if len(user_detail) > 0:
                    user_role = user_detail[0]["user_role"]
                    user_id = user_detail[0]['user_id']
                if type == 'payment':
                    quantity = metadata.get('quantity')
                    date_extend = metadata.get('date_extend')
                    is_date_extend = metadata.get('is_date_extend')
                    job_id = metadata.get('job_id')
                    if user_role == 'partner':
                        if is_date_extend == "0":
                            try: 
                                # query = "insert into partner_additional_posts (user_id, posts_count, post_status) values (%s,%s,%s)"
                                # values = (user_id, int(quantity), 'opened',)
                                # row_count = update_query(query, values)
                                query = "UPDATE user_plan_details SET no_of_jobs = no_of_jobs + %s, total_jobs = total_jobs + %s, additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                                values = (int(quantity), int(quantity), int(quantity), user_id,)
                                row_count = update_query(query,values)
                                if row_count > 0:
                                    print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                    query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                    created_at = datetime.now()                    
                                    values = (user_id,"You have successfully added an additional post(s) to your account!",created_at,)
                                    update_query(query,values)
                            except Exception as e:
                                print("Payment webhook error : "+str(e))
                                return {"status" : "success"}    
                        # else:
                        #     try: 
                        #         created_at = datetime.now()     
                        #         query = "UPDATE learning SET days_left = calc_day + %s, created_at = %s, is_active = %s, post_status = %s WHERE id = %s"
                        #         values = (int(date_extend),created_at,"Y","opened",job_id,)
                        #         row_count = update_query(query,values)
                        #         if row_count > 0:
                        #             print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                        #             query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                        #             created_at = datetime.now()                    
                        #             values = (user_id,"Date extended",created_at,)
                        #             update_query(query,values)
                        #     except Exception as e:
                        #         print("Payment webhook error : "+str(e))
                        #         return {"status" : "success"}
                    elif user_role == 'employer':
                        try: 
                            query = "UPDATE user_plan_details SET additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                            values = (int(quantity), user_id,)
                            row_count = update_query(query,values)
                            print(f"row count : {row_count}")
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (user_id,"You have successfully added an additional job(s) to your account!",created_at,)
                                update_query(query,values)
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return {"status" : "success"}
                    else:
                        return {"status" : "failed"}
                else:
                    return {"status" : "failed"}
            except Exception as err:
                print("Error in checkout.session.completed: "+str(err))
                return {"status" : "failed"}
            
        if event_type == 'customer.subscription.created':
            try:
                metadata = data_object.get('metadata', {})
                discount_details = data_object.get('discount', {})
                email_id = metadata.get("user_id", None)
                from_trial = metadata.get("from_trial", "no")
                from_upgrade = metadata.get("from_upgrade", "no")
                currency = data_object['currency']
                query = 'SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;'
                values = (email_id,)
                user_id_dict = execute_query(query,values)
                user_id = 0
                user_role = ''
                if user_id_dict:
                    user_id = user_id_dict[0]['user_id']
                    user_role = user_id_dict[0]['user_role']
                update_currency = 'update users set payment_currency = %s where email_id = %s'
                values = (currency, email_id,)
                update_query(update_currency,values)

                update_currency = 'update sub_users set payment_currency = %s where user_id = %s'
                values = (currency, user_id,)
                update_query(update_currency,values)

                if from_trial == "yes" or discount_details != None:
                    plan = metadata.get('plan', None)
                    existing_pricing_key = metadata.get('existing_pricing_key', '')
                    current_period_start = data_object['current_period_start']
                    current_period_end = data_object['current_period_end']
                    existing_pricing = metadata.get('existing_plan', None)

                    checkout_status = data_object['status']
                    subscription_id = data_object['id']

                    query = 'SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;'
                    values = (email_id,)
                    user_id_dict = execute_query(query,values)
                    if not user_id_dict:
                        query = 'SELECT su.user_id, su.email_id, ur.user_role FROM sub_users su JOIN user_role ur ON su.role_id = ur.role_id WHERE su.email_id = %s;'
                        values = (email_id,)
                        user_id_dict = execute_query(query,values)
                    # user_id = 0
                    # user_role = ''
                    if user_id_dict:
                        try:
                            user_id = user_id_dict[0]['user_id']
                            user_role = user_id_dict[0]['user_role']
                            update_pricing_query = 'update users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s where email_id = %s'
                            update_pricing_values = (checkout_status, subscription_id, plan, existing_pricing, 'Y', email_id,)
                            update_query(update_pricing_query, update_pricing_values,)

                            update_sub_users = 'update sub_users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s where user_id = %s'
                            sub_users_values = (checkout_status, subscription_id, plan, existing_pricing, 'Y', user_id,)
                            update_query(update_sub_users, sub_users_values,)

                            if discount_details != None:
                                update_discount_query = 'update users set existing_pricing_key = %s, current_period_start = %s, current_period_end = %s where email_id = %s'
                                update_discount_values = (existing_pricing_key, current_period_start, current_period_end, email_id,)
                                update_query(update_discount_query, update_discount_values)

                                update_sub_users_query = 'update sub_users set existing_pricing_key = %s, current_period_start = %s, current_period_end = %s where user_id = %s'
                                update_sub_users_values = (existing_pricing_key, current_period_start, current_period_end, user_id,)
                                update_query(update_sub_users_query, update_sub_users_values)

                            created_at = datetime.now()
                            insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            insert_notification_values = (user_id, "Thank you for choosing our job portal. We’re excited to have you on board and look forward to supporting your hiring journey.", created_at,)
                            update_query(insert_notification_query,insert_notification_values,)
                            if user_role == 'employer':
                                background_runner.get_employer_details(user_id)
                            if user_role == 'partner':
                                background_runner.get_partner_details(user_id)
                            # if user_role == 'employer' or user_role == 'partner':
                            #     if user_role == 'employer' and discount_details != None:
                            #         if plan == 'Basic':
                            #             job_count = 2
                            #         elif plan == 'Premium':
                            #             job_count = 5
                            #         else:
                            #             job_count = 10
                            #     elif user_role == 'partner':
                            #         if plan == 'Basic':
                            #             job_count = 4
                            #         else:
                            #             job_count = 0
                            #     else:
                            #         job_count = 0
                            # query = "update user_plan_details set total_jobs = %s, user_plan = %s, no_of_jobs = %s where user_id = %s"
                            # update_values = (job_count, plan, job_count, user_id,)
                            # update_query(query,update_values)
                            checkout_status_msg = 'Subscription created'
                            print(f"subscription_id : {subscription_id}, checkout_status : {checkout_status}, checkout_status_msg : {checkout_status_msg}")
                            return jsonify({"status" : "success"})
                        except Exception as err:
                            print(f"Error in trial webhook DB updation : {err}")
                            return jsonify({"status" : "failed"})
                    else:
                        print(f"user id dict is empty: {user_id_dict}")
                elif from_upgrade == "Yes":
                    plan = metadata.get('plan', None)
                    existing_pricing_key = metadata.get('existing_pricing_key', '')
                    current_period_start = data_object['current_period_start']
                    current_period_end = data_object['current_period_end']
                    existing_pricing = metadata.get('existing_plan', None)

                    checkout_status = data_object['status']
                    subscription_id = data_object['id']

                    query = 'SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;'
                    values = (email_id,)
                    user_id_dict = execute_query(query,values)
                    if not user_id_dict:
                        query = 'SELECT su.user_id, su.email_id, ur.user_role FROM sub_users su JOIN user_role ur ON su.role_id = ur.role_id WHERE su.email_id = %s;'
                        values = (email_id,)
                        user_id_dict = execute_query(query,values)
                    # user_id = 0
                    # user_role = ''
                    if user_id_dict:
                        # print(f"User id dict:{user_id_dict}")
                        try:
                            user_id = user_id_dict[0]['user_id']
                            user_role = user_id_dict[0]['user_role']
                            update_pricing_query = 'update users set payment_status = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s where email_id = %s'
                            update_pricing_values = (checkout_status, existing_pricing_key, current_period_start, current_period_end, subscription_id, plan, existing_pricing, 'Y', email_id,)
                            update_query(update_pricing_query, update_pricing_values,)

                            update_sub_users = 'update sub_users set payment_status = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s where user_id = %s'
                            sub_users_values = (checkout_status, existing_pricing_key, current_period_start, current_period_end, subscription_id, plan, existing_pricing, 'Y', user_id,)
                            update_query(update_sub_users, sub_users_values,)

                            # if discount_details != None:
                            #     update_discount_query = 'update users set existing_pricing_key = %s, current_period_start = %s, current_period_end = %s where email_id = %s'
                            #     update_discount_values = (existing_pricing_key, current_period_start, current_period_end, email_id,)
                            #     update_query(update_discount_query, update_discount_values)

                            #     update_sub_users_query = 'update sub_users set existing_pricing_key = %s, current_period_start = %s, current_period_end = %s where user_id = %s'
                            #     update_sub_users_values = (existing_pricing_key, current_period_start, current_period_end, user_id,)
                            #     update_query(update_sub_users_query, update_sub_users_values)

                            created_at = datetime.now()
                            insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            insert_notification_values = (user_id, "Thank you for choosing our job portal. We’re excited to have you on board and look forward to supporting your hiring journey.", created_at,)
                            update_query(insert_notification_query,insert_notification_values,)
                            if user_role == 'employer':
                                background_runner.get_employer_details(user_id)
                            if user_role == 'partner':
                                background_runner.get_partner_details(user_id)
                            if user_role == 'employer' or user_role == 'partner':
                                if user_role == 'employer':
                                    if plan == 'Basic':
                                        job_count = 2
                                    elif plan == 'Premium':
                                        job_count = 5
                                    else:
                                        job_count = 10
                                elif user_role == 'partner':
                                    if plan == 'Basic':
                                        job_count = 4
                                    else:
                                        job_count = 0
                                else:
                                    job_count = 0
                            query = "update user_plan_details set total_jobs = %s, user_plan = %s, no_of_jobs = %s, additional_jobs_count = %s where user_id = %s"
                            update_values = (job_count, plan, job_count, 0, user_id,)
                            update_query(query,update_values)
                            checkout_status_msg = 'Subscription created'
                            print(f"subscription_id : {subscription_id}, checkout_status : {checkout_status}, checkout_status_msg : {checkout_status_msg}")
                            return jsonify({"status" : "success"})
                        except Exception as err:
                            print(f"Error in trial webhook DB updation : {err}")
                            return jsonify({"status" : "failed"})
                    else:
                        print(f"user id dict is empty: {user_id_dict}")
                return jsonify({"status" : "success"})
            except Exception as err:
                print(f"Error in created webhook : {err}")
                return jsonify({"status" : "failed"})
        
        if event_type == 'customer.subscription.trial_will_end':
            metadata = data_object.get("metadata", {})
            email_id = metadata.get("user_id") or metadata.get("email_id")
            user_details_query = "SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;"
            user_details_values = (email_id,)
            user_detail = execute_query(user_details_query, user_details_values)
            user_id = 0
            user_role = ''
            if len(user_detail) > 0:
                user_role = user_detail[0]["user_role"]
                user_id = user_detail[0]['user_id']
            created_at = datetime.now()
            if user_role == 'employer' or user_role == 'partner':
                insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                insert_notification_values = (user_id, "Your trial period is nearing its end. To continue accessing all the features and opportunities we offer, we encourage you to subscribe and stay with us on your journey.", created_at)
                update_query(insert_notification_query,insert_notification_values)

                plan = data_object['metadata']['plan']
                checkout_status = data_object['status']
                subscription_id = data_object['id']
                background_runner.send_plan_end_email(email_id)

            checkout_status_msg = 'Subscription trial will end'
        
        if event_type == 'customer.subscription.deleted':
            current_period_start = data_object['current_period_start']
            current_period_end = data_object['current_period_end']
            metadata = data_object.get('metadata', {})
            current_sub_item_id = data_object['items']['data'][0]['id']
            print(f"current_sub_item_id : {current_sub_item_id}")
            from_upgrade = metadata.get('from_upgrade', "no")
            checkout_status = data_object['status']
            subscription_id = data_object['id']
            email_id = metadata.get("user_id", None)
            from_trial = metadata.get("from_trial", "no")
            upgrade_cancel_flag = metadata.get('upgrade_cancel_flag', 'false')
            user_details_query = "SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;"
            user_details_values = (email_id,)
            user_detail = execute_query(user_details_query, user_details_values)

            user_id = 0
            user_role = ''
            custom_payment_status = None
            if len(user_detail) > 0:
                user_id = user_detail[0]['user_id']
                user_role = user_detail[0]['user_role']
            if from_trial == 'yes' and checkout_status != 'trialing':
                custom_payment_status = 'trial_expired'
            elif from_upgrade == 'Yes' and checkout_status == 'canceled':
                custom_payment_status = 'canceled'
            # else:
            #     custome_payment_status = checkout_status
            if custom_payment_status and upgrade_cancel_flag == 'false':
                print(f"checkout_status in trial expired: {checkout_status}")
                update_pricing_query = 'update users set payment_status = %s where email_id = %s'
                update_pricing_values = (custom_payment_status, email_id,)
                update_query(update_pricing_query, update_pricing_values)

                update_sub_users = 'update sub_users set payment_status = %s where user_id = %s'
                sub_users_values = (custom_payment_status, user_id,)
                update_query(update_sub_users, sub_users_values)
                if user_role == 'employer':
                    update_user_plan = 'update user_plan_details set no_of_jobs = %s, total_jobs = %s where user_id = %s'
                    update_user_plan_values = (0, 0, user_id,)
                    update_query(update_user_plan, update_user_plan_values)
                    background_runner.get_employer_details(user_id)
                elif user_role == 'partner':
                    update_user_plan = 'update user_plan_details set no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s where user_id = %s'
                    update_user_plan_values = (0, 0, 0, user_id,)
                    update_query(update_user_plan, update_user_plan_values)
                    background_runner.get_partner_details(user_id)

                created_at = datetime.now()
                notification_msg = f"Your current plan has been expired. Please subscribe to continue accessing the features we offer."
                insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
                insert_notification_values = (user_id, notification_msg, created_at)
                update_query(insert_notification_query, insert_notification_values)
                return jsonify({"status" : "success"})
        
        if event_type == 'customer.subscription.updated':
            try:
                current_period_start = data_object['current_period_start']
                current_period_end = data_object['current_period_end']
                metadata = data_object.get('metadata', {})
                current_sub_item_id = data_object['items']['data'][0]['id']
                # print(f"current_sub_item_id : {current_sub_item_id}")
                # print(f"data object{data_object}")
                from_upgrade = metadata.get('from_upgrade', "no")
                checkout_status = data_object['status']
                subscription_id = data_object['id']
                cancel_at_period_end = data_object['cancel_at_period_end']
                email_id = metadata.get("user_id", None)
                from_trial = metadata.get("from_trial", "no")
                is_canceled = metadata.get("is_canceled", "no")
                from_downgrade = metadata.get("from_downgrade", "no")
                # print(f"From downgrade :: {from_downgrade}")
                user_details_query = "SELECT u.user_id, u.email_id, ur.user_role FROM users u JOIN user_role ur ON u.user_role_fk = ur.role_id WHERE u.email_id = %s;"
                user_details_values = (email_id,)
                user_detail = execute_query(user_details_query, user_details_values)
                upgrade_cancel_flag = metadata.get('upgrade_cancel_flag', 'false')

                user_id = 0
                user_role = ''
                if len(user_detail) > 0:
                    user_id = user_detail[0]['user_id']
                    user_role = user_detail[0]['user_role']
                if (from_upgrade == "Yes" and (cancel_at_period_end == False and is_canceled == "no") and upgrade_cancel_flag == 'false') or from_downgrade == 'yes':
                    # print(f"metadata {metadata} \n email_id {email_id}")
                    plan = metadata.get('plan', None)
                    existing_pricing = metadata.get('existing_plan', None) 
                    existing_subscription_id = metadata.get('existing_subscription_id', '')
                    existing_pricing_key = metadata.get('existing_pricing_key', '')
                    
                    update_pricing_query = 'update users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, current_sub_item_id = %s, is_cancelled = %s  where email_id = %s'
                    update_pricing_values = (checkout_status, subscription_id, plan, existing_pricing, existing_pricing_key, 'Y', current_period_start, current_period_end, current_sub_item_id, 'N', email_id,)
                    update_query(update_pricing_query, update_pricing_values)

                    update_sub_users = 'update sub_users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, current_sub_item_id = %s, is_cancelled = %s where user_id = %s'
                    sub_users_values = (checkout_status, subscription_id, plan, existing_pricing, existing_pricing_key, 'Y', current_period_start, current_period_end, current_sub_item_id, 'N', user_id,)
                    update_query(update_sub_users, sub_users_values)

                    created_at = datetime.now()
                    notification_msg = ''
                    get_old_plan_query = "select old_plan from users where email_id = %s"
                    get_old_plan_values = (email_id,)
                    old_plan = execute_query(get_old_plan_query, get_old_plan_values)
                    if len(old_plan) > 0:
                        old_plan = old_plan[0]['old_plan']
                        if old_plan == '' or old_plan == None:
                            notification_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {plan} plan!"
                        else:
                            notification_msg = f"You have successfully signed up for the {plan} plan!"
                    insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
                    insert_notification_values = (user_id, notification_msg, created_at)
                    update_query(insert_notification_query, insert_notification_values)
                    if user_role == 'employer':
                        background_runner.get_employer_details(user_id)
                    elif user_role == 'partner':
                        background_runner.get_partner_details(user_id)
                    job_count = 0
                    role_id = 0
                    if user_role == 'employer':
                        if plan == 'Basic':
                            job_count = 2
                        elif plan == 'Premium':
                            job_count = 5
                        else:
                            job_count = 10
                    elif user_role == 'partner':
                        if plan == "Basic":
                            job_count = 4
                    query = "UPDATE user_plan_details SET total_jobs = %s, user_plan = %s, no_of_jobs = %s, additional_jobs_count = %s WHERE user_id = %s;"
                    values = (job_count, plan, job_count, 0, user_id,)
                    update_query(query, values)
                    if from_upgrade == 'Yes':
                            if existing_subscription_id:
                                try:
                                    stripe.Subscription.modify(existing_subscription_id, metadata={"action": "upgrading", "upgrade_cancel_flag" : "true"})
                                    stripe.Subscription.delete(existing_subscription_id)
                                    print(f"Successfully canceled subscription {existing_subscription_id}.")
                                except stripe.error.StripeError as e:
                                    print(f"Error canceling subscription: {e.user_message}")
                            else:
                                print("No trial subscription ID found in metadata.")
                    return jsonify({"status" : "success"})
                
                ########################################
                # elif from_upgrade == "Yes" and (cancel_at_period_end == False):
                #     update_cancel_flag = 'update users set is_cancelled = %s where user_id = %s'
                #     update_cancel_values = ('N', user_id,)
                #     update_query(update_cancel_flag, update_cancel_values)

                #     update_cancel_sub_user_flag = 'update sub_users set is_cancelled = %s where user_id = %s'
                #     update_cancel_sub_user_values = ('N', user_id,)
                #     update_query(update_cancel_sub_user_flag, update_cancel_sub_user_values)
                #     return jsonify({"status" : "success"})
                
                elif from_upgrade == "Yes" and (cancel_at_period_end == True and is_canceled == "yes"):
                    update_cancel_flag = 'update users set is_cancelled = %s where user_id = %s'
                    update_cancel_values = ('Y', user_id,)
                    update_query(update_cancel_flag, update_cancel_values)

                    update_cancel_sub_user_flag = 'update sub_users set is_cancelled = %s where user_id = %s'
                    update_cancel_sub_user_values = ('Y', user_id,)
                    update_query(update_cancel_sub_user_flag, update_cancel_sub_user_values)
                    return jsonify({"status" : "success"})
            except Exception as err:
                print("Error in subscription updated:", err)
                return jsonify({"status" : "failed"})
    except Exception as e:
        print("Error in webhook_session:", e)
        return jsonify({"status" : "failed"})
    finally:
        return jsonify({"status" : "success"})

def create_trial_session(user_email_id, day, lookup_key, product_plan):
    try:
        encoded_usr_email = b64encode(user_email_id.encode("utf-8")).decode("utf-8")
        
        # Update user process flag
        query = "UPDATE users SET process_flag = %s WHERE email_id = %s"
        values = (1, user_email_id,)
        row_count = update_query(query, values)

        if row_count > 0:
            # Create or retrieve the customer by email
            customer = None
            try:
                customer_list = stripe.Customer.list(email=user_email_id)
                if customer_list.data:
                    customer = customer_list.data[0]
                else:
                    customer = stripe.Customer.create(
                        email=user_email_id,
                        metadata={"email_id": user_email_id}
                    )
                    try:
                        temp_dict = {'Message': f"Customer {user_email_id} successfully created in Stripe."}
                        event_properties = background_runner.process_dict(encoded_usr_email, "Creating Stripe Customer", temp_dict)
                        background_runner.mixpanel_event_async(encoded_usr_email,"Creating Stripe Customer",event_properties, temp_dict.get('Message'), user_data=None)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Creating Stripe Customer, {str(e)}") 
            except Exception as e:
                try:
                    temp_dict = {'Exception' : str(e),
                                'Message': "Error in creating customer in Stripe."}
                    event_properties = background_runner.process_dict(encoded_usr_email, "Creating Stripe Customer Error", temp_dict)
                    background_runner.mixpanel_event_async(encoded_usr_email,"Creating Stripe Customer Error",event_properties, temp_dict.get('Message'), user_data=None)
                except Exception as error:  
                    print(f"Error in mixpanel event logging: Creating Stripe Customer Error, {str(error)}")
                print("An error occurred while creating customer in Stripe: "+str(e))
            # Create the subscription
            subscription = stripe.Subscription.create(
                customer=customer.id,  # Pass the customer ID here
                items=[{
                    'price': lookup_key,
                    'quantity': 1,
                }],
                trial_period_days=day,
                metadata={
                    "email_id": user_email_id,
                    "pricing_category": product_plan
                },
                payment_behavior='default_incomplete',  # Create subscription without immediate payment
                trial_settings={"end_behavior": {"missing_payment_method": "cancel"}},
                expand=['latest_invoice.payment_intent']  # Optionally expand to retrieve payment intent details
            )
            try:
                temp_dict = {'Message': f"Customer {user_email_id} successfully created subscription in Stripe.",
                             'Trial Period' : day,
                             'Pricing Category' : product_plan}
                event_properties = background_runner.process_dict(encoded_usr_email, "Creating Stripe Customer Subscription", temp_dict)
                background_runner.mixpanel_event_async(encoded_usr_email,"Creating Stripe Customer Subscription",event_properties, temp_dict.get('Message'), user_data=None)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Creating Stripe Customer Subscription, {str(e)}")
            # Return the subscription ID instead of a URL
            return  subscription.id
        else:
            try:
                temp_dict = {'Message': f"Subscription process not initialized by customer {user_email_id} in Stripe."}
                event_properties = background_runner.process_dict(encoded_usr_email, "Creating Stripe Customer Subscription", temp_dict)
                background_runner.mixpanel_event_async(encoded_usr_email,"Creating Stripe Customer Subscription",event_properties, temp_dict.get('Message'), user_data=None)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Creating Stripe Customer Subscription, {str(e)}")
            return False

    except Exception as e:
        try:
            temp_dict = {'Exception' : str(e),
                        'Message': "Error in creating customer subscription."}
            event_properties = background_runner.process_dict(encoded_usr_email, "Creating Stripe Customer Subscription Error", temp_dict)
            background_runner.mixpanel_event_async(encoded_usr_email,"Creating Stripe Customer Subscription Error",event_properties, temp_dict.get('Message'), user_data=None)
        except Exception as error:  
            print(f"Error in mixpanel event logging: Creating Stripe Customer Subscription Error, {str(error)}")
        print("Initialize subscription error: " + str(e))
        query = "UPDATE users SET process_flag = %s WHERE email_id = %s"
        values = (3, user_email_id,)
        row_count = update_query(query, values)
        return False

def create_payment_session(request):
    try:
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]  
            user_data = get_user_data(user_email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
                if user_data["is_exist"]:
                    query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                    values = (user_email_id,)
                    email_id_dict = execute_query(query, values)
                    if email_id_dict:
                        user_email_id = email_id_dict[0]['email_id']
            # Get the data from the request
            data = request.get_json()
            if 'lookup_key' not in data:
                return api_json_response_format(False, "Lookup key is required.", 204, {})
            lookup_key = data.get('lookup_key')
            if lookup_key:
                get_price_id = 'select attribute_value from payment_config where attribute_name = %s'
                values = (lookup_key,)
                price_id = execute_query(get_price_id, values)
                if price_id:
                    price_id = price_id[0]['attribute_value']  
                else:
                    return api_json_response_format(False, "Invalid lookup key.", 204, {})
            else:
                return api_json_response_format(False, "Lookup key is required.", 204, {})
            quantity = data.get('no_of_jobs', 1)
            date_extend = 45
            is_date_extend = data.get('is_date_extend', 1)
            job_id = data.get('job_id', "")
            product = data.get('product', "")
            user = data.get('user', "")

            # Create a Checkout Session
            checkout_session = stripe.checkout.Session.create(
                line_items=[
                    {
                        'price': price_id,  # Replace 'lookup_key' with your specific price ID
                        'quantity': quantity,
                    },
                ],
                customer_email=user_email_id,
                allow_promotion_codes=True,
                mode='payment',  # Changed from 'subscription' to 'payment' '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,
                success_url=WEB_APP_URI + '/checkout_success?flag=success&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + user_email_id,
                cancel_url=WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + user_email_id,
                metadata={
                    "type" : 'payment',
                    "email_id" : user_email_id,
                    "quantity":quantity,
                    "date_extend":date_extend,
                    "is_date_extend":is_date_extend,
                    "job_id":job_id,
                    "product":product,
                    "user":user,
                    "pricing_category":""
                }
            )
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Lookup Key' : lookup_key,
                            'price_id' : price_id,
                            'Message': f'Payment session successfully initialized for {user_email_id}'}
                event_properties = background_runner.process_dict(user_data["email_id"], "Creating Payment Session", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Creating Payment Session",event_properties, temp_dict.get('Message'), user_data=None)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Creating Payment Session, {str(e)}")          
            return api_json_response_format(True,"success",0,{"url":checkout_session.url})
        else:
            return api_json_response_format(False,"Invalid token. Please try again.",401,{}) 

    except Exception as e:
        try:
            temp_dict = {'Exception' : str(e),
                        'Message': "Error in creating payment session."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Creating Payment Session Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Creating Payment Session Error",event_properties, temp_dict.get('Message'), user_data=None)
        except Exception as error:  
            print(f"Error in mixpanel event logging: Creating Payment Session Error, {str(error)}")
        print("Initialize payment error : "+str(e))
        return api_json_response_format(False,"Sorry, we are unable to process the payment. We request you to try again.",403,{}) 

def updateCheckoutStatusPartner(request):    
        try: 
            request_json = request.get_json()
            role_id = 0
            if not request_json:
                return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})
            
            flag = str(request_json["flag"])            
            email_id_encoded = str(request_json["user_id"])                        
                 
            if not email_id_encoded:
                    return api_json_response_format(False,"Sorry, we are unable to process the payment. We request you to try again.",403,{}) 
            
            # email_id = b64decode(email_id_encoded.encode("utf-8")).decode("utf-8")    
            email_id = email_id_encoded        
            query = "UPDATE users SET process_flag = %s WHERE email_id = %s"
            values = (2,email_id,)
            update_query(query,values)
            query = "select user_role_fk from users where email_id=%s"
            values = (email_id,)
            role_obj = execute_query(query,values)
            if len(role_obj) >0:
                role_id = role_obj[0]["user_role_fk"]
                
            if flag == "success":                
                return api_json_response_format(True,"Thank you "+str(email_id)+" for subscribing with us.",0,{"user_role":role_id}) 
            else:
                return api_json_response_format(True,"Sorry "+str(email_id)+", but your checkout process has failed. Please try again.",0,{"user_role":role_id}) 

        except Exception as e:
            print("Error in updating the checkout status request : %s",str(e))             
            return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})

def webhook_received():
    try:           
        request_data = json.loads(request.data)
        pricing_category = ""
        user=""
        Job_id=""
        is_date_extend=2
        date_extend=30
        Quantity=1
        email_id=""
        subscription_id=""

        if WH_SEC_KEY:
            signature = request.headers.get('stripe-signature')
            try:
                event = stripe.Webhook.construct_event(payload=request.data, sig_header=signature, secret=WH_SEC_KEY)
                data = event['data']
            except Exception as e:
                print("Webhook error : "+str(e))
                return api_json_response_format(False,"failed",403,{})                 
            event_type = event['type']
        else:
            data = request_data['data']
            event_type = request_data['type']

        data_object = data['object']        
        event_type = event['type'] 
        
        if  event_type =="customer.subscription.deleted":
            checkout_status_msg = 'Subscription cancelled.'
            subscription = event['data']['object']
            checkout_status = data_object['status']
            query = "SELECT user_id FROM users  where subscription_id = %s "            
            values = (subscription.id,)
            rs = execute_query(query,values)
            if len(rs) > 0:
                user_id = rs[0]["user_id"]  
                query = "UPDATE users SET subscription_id = %s,pricing_category = %s,payment_status = %s,process_flag = %s WHERE user_id = %s"
                values = (subscription.id,"Basic",checkout_status,3,user_id,)
                row_count = update_query(query,values)


        if event_type == 'customer.subscription.created':
            checkout_status_msg = 'Subscription created successfully.'
            subscription = event['data']['object']
            metadata = data_object.get('metadata', {})
            email_id = metadata.get('email_id')
            checkout_status = data_object['status']
            query = "UPDATE users SET subscription_id = %s,pricing_category = %s,payment_status = %s,process_flag = %s WHERE email_id = %s"
            values = (subscription.id,"Basic",checkout_status,2,email_id,)
            row_count = update_query(query,values)


        if event_type == 'checkout.session.completed':
            subscription_id = data_object['subscription']
            email_id = data_object['customer_email']
            metadata = data_object.get('metadata', {})
            print(f"Metadata in Webhook: {metadata}")
            
            Quantity = metadata.get('quantity')
            date_extend = metadata.get('date_extend')
            is_date_extend = metadata.get('is_date_extend')
            Job_id = metadata.get('Job_id')
            user = metadata.get('user')
            pricing_category = metadata.get('pricing_category')

             
            if subscription_id:
                query = "SELECT user_id FROM users  where email_id = %s "            
                values = (email_id,)
                rs = execute_query(query,values)             
                if len(rs) > 0:
                    user_id = rs[0]["user_id"]     
                    query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                    created_at = datetime.now()
                    notifi_message = "Payment received successfully."
                    values = (user_id,notifi_message,created_at,)
                    update_query(query,values)
                    checkout_status = data_object['status']
                    subscription_id = data_object['id']
                    
                    checkout_status_msg = checkout_status
                    
                    if event_type == 'customer.subscription.trial_will_end':
                        checkout_status_msg = 'Subscription trial will end'
                    elif event_type == 'customer.subscription.created':
                        checkout_status_msg = 'Subscription created successfully.'
                    elif event_type == 'customer.subscription.updated':
                        checkout_status_msg = 'Subscription updated successfully.'
                    elif event_type == 'customer.subscription.deleted':            
                        checkout_status_msg = 'Subscription canceled successfully.'
                    else:
                        checkout_status_msg = event_type        

                    print(pricing_category)

                    if not pricing_category == "":
                        print("test")
                        try: 
                            checkout_status = data_object['status']
                            subscription_id = data_object['id']
                            print(f"subscription_id: {subscription_id}, pricing_category: {pricing_category}, checkout_status: {checkout_status}, email_id: {email_id}")
                            query = "UPDATE users SET subscription_id = %s,pricing_category = %s,payment_status = %s WHERE email_id = %s"
                            values = (subscription_id,pricing_category,checkout_status,email_id,)
                            row_count = update_query(query,values)
                            print(f"subscription_id: {subscription_id}, pricing_category: {pricing_category}, checkout_status: {checkout_status}, email_id: {email_id}")
                            print("row_count :"+str(row_count))
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Subscription ID : "+str(subscription_id)+", Message : "+str(checkout_status_msg))
                                query = "SELECT user_id FROM users  where email_id = %s "            
                                values = (email_id,)
                                rs = execute_query(query,values)             
                                if len(rs) > 0:
                                    user_id = rs[0]["user_id"]     
                                    query = "SELECT user_role_fk FROM `users` WHERE user_id = %s"                  
                                    values = (user_id,)
                                    res = execute_query(query,values)
                                    if len(res) > 0:
                                        role_id = res[0]["user_role_fk"] 
                                        print("role_id :"+str(role_id))
                                        if role_id == 2:
                                            if pricing_category == "Premium":
                                                no_of_jobs = 5
                                            elif pricing_category == "Platinum":
                                                no_of_jobs = 10
                                            else:
                                                no_of_jobs = 2
                                        elif role_id == 6:
                                            if pricing_category == "Premium":
                                                no_of_jobs = 5
                                            elif pricing_category == "Platinum":
                                                no_of_jobs = 10
                                            else:
                                                no_of_jobs = 1

                                    query = "UPDATE user_plan_details SET user_plan=%s,no_of_jobs = %s,total_jobs = %s WHERE user_id = %s"
                                    values = (pricing_category,no_of_jobs,no_of_jobs,user_id,)
                                    update_query(query,values)
                                    print("user_plan :"+str(pricing_category))
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return api_json_response_format(False,"failed",403,{})  
                
                return api_json_response_format(True,"success",403,{})
            else:
                query = "SELECT user_id FROM users  where email_id = %s "            
                values = (email_id,)
                rs = execute_query(query,values)             
                if len(rs) > 0:
                    user_id = rs[0]["user_id"]     
                    query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                    created_at = datetime.now()
                    notifi_message = "Payment received successfully."
                    values = (user_id,notifi_message,created_at,)
                    update_query(query,values)
                if user == "Employer":
                    if is_date_extend == "0":
                        try: 
                            query = "UPDATE user_plan_details SET  no_of_jobs = no_of_jobs + %s,total_jobs=total_jobs+%s WHERE user_id = %s"
                            values = (int(Quantity),int(Quantity),user_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (user_id,"Job Added",created_at,)
                                update_query(query,values)
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return api_json_response_format(False,"failed",403,{})    
                    else:
                        try:   
                            created_at = datetime.now() 
                            query = "UPDATE job_post SET days_left = calc_day + %s,created_at=%s,is_active=%s,job_status=%s  WHERE id = %s"
                            values = (int(date_extend),created_at,"Y","opened",Job_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (user_id,"Date extented",created_at,)
                                update_query(query,values)
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return api_json_response_format(False,"failed",403,{})   
                else:
                    query = "SELECT user_id FROM users  where email_id = %s "            
                    values = (email_id,)
                    rs = execute_query(query,values)             
                    if len(rs) > 0:
                        user_id = rs[0]["user_id"]  
                    if is_date_extend == "0":
                        try: 
                            query = "UPDATE user_plan_details SET  no_of_jobs = no_of_jobs + %s,total_jobs=total_jobs+%s WHERE user_id = %s"
                            values = (int(Quantity),int(Quantity),user_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (user_id,"Job Added",created_at,)
                                update_query(query,values)
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return api_json_response_format(False,"failed",403,{})    
                    else:
                        try: 
                            created_at = datetime.now()     
                            query = "UPDATE learning SET days_left = calc_day + %s,created_at=%s,is_active=%s,post_status=%s WHERE id = %s"
                            values = (int(date_extend),created_at,"Y","opened",Job_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (user_id,"Date extented",created_at,)
                                update_query(query,values)
                        except Exception as e:
                            print("Payment webhook error : "+str(e))
                            return api_json_response_format(False,"failed",403,{}) 
            return api_json_response_format(True,"success",403,{}) 
        return api_json_response_format(True,"success",403,{}) 
    
    except Exception as e:
        print("Payment webhook error : "+str(e))
        return api_json_response_format(False,"failed",403,{}) 

    
def cancel_subscription(request):    
        try: 
            token_result = get_user_token(request)                       
            if token_result["status_code"] == 200:  
                user_email_id = token_result["email_id"]
                user_data = get_user_data(user_email_id)
                if not user_data['is_exist']:
                    user_data = get_sub_user_data(user_email_id)
                    if user_data["is_exist"]:
                        query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                        values = (user_email_id,)
                        email_id_dict = execute_query(query, values)
                        if email_id_dict:
                            user_email_id = email_id_dict[0]['email_id']
                query = 'SELECT subscription_id,user_id FROM users WHERE email_id = %s'
                values = (user_email_id,)
                qry_result = execute_query(query, values)
                if qry_result:
                    subscription_id = qry_result[0]["subscription_id"]
                    user_id = qry_result[0]["user_id"]     
                    stripe.Subscription.modify(
                        subscription_id,
                        cancel_at_period_end = True,
                        metadata={"is_canceled" : "yes"}
                    )                   
                    query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                    created_at = datetime.now()                    
                    values = (user_id,"You have successfully cancelled your subscription.",created_at,)
                    update_query(query,values)
                    background_runner.send_plan_cancelled_email(user_email_id)
                    return api_json_response_format(True,"Hi "+str(user_email_id)+", You have successfully cancelled your subscription.",0,{})
                else:
                    return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})
            else:
                return api_json_response_format(False,"Invalid token.",403,{})                             
        except Exception as ex:                
            print("Error in cancelling the subscription, Error : "+str(ex))
            return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})                             

def get_country_name(lat, lon):
    """
    Function to get the country name from latitude and longitude.
    
    :param lat: Latitude as a float
    :param lon: Longitude as a float
    :return: Country name as a string, or None if not found
    """
    geolocator = Nominatim(user_agent="adrageoapi")  # Use a unique user-agent
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True)
        if location and 'country' in location.raw['address']:
            return location.raw['address']['country']
    except Exception as e:
        print(f"Error retrieving location: {e}")
    return None

def get_checkout_status(request):    
        try: 
            token_result = get_user_token(request)                       
            if token_result["status_code"] == 200:  
                user_email_id = token_result["email_id"]            
                user_data = get_user_data(user_email_id)
                if not user_data['is_exist']:
                    user_data = get_sub_user_data(user_email_id)
                    if user_data["is_exist"]:
                        query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                        values = (user_email_id,)
                        email_id_dict = execute_query(query, values)
                        if email_id_dict:
                            user_email_id = email_id_dict[0]['email_id']
                user_id = user_data.get("user_id")
                query = 'SELECT country, payment_status, pricing_category, is_trial_started, is_cancelled, payment_currency, current_period_end FROM users WHERE email_id = %s'
                values = (user_email_id,)
                qry_result = execute_query(query, values)

                check_customer_query = 'select id from stripe_customers where email = %s'
                values = (user_email_id,)
                customer_result = execute_query(check_customer_query, values)

                user_payment_profile_query = "SELECT id FROM user_payment_profiles WHERE email_id = %s AND gateway = %s"
                values = (user_email_id, 'stripe',)
                payment_profile_result = execute_query(user_payment_profile_query, values)

                if customer_result or payment_profile_result:
                    from_stripe = "yes"
                else:
                    from_stripe = "no"
                
                
                # res_data = request.get_json()
                # if 'lat' not in res_data or 'lon' not in res_data:
                #     return jsonify({"error": "Please provide both latitude and longitude"}), 400

                # lat = res_data["lat"]
                # lon = res_data["lon"]
                
                # country = get_country_name(lat, lon)
                query = "SELECT validity_year FROM subscriptions WHERE user_id = %s ORDER BY id DESC LIMIT 1"
                result = execute_query(query, (user_id,))
                validity_year = 1
                if result:
                    validity_year = result[0]['validity_year']

                no_of_jobs = 0
                assisted_jobs_left = 0

                query = "SELECT * FROM user_plan_details WHERE user_id = %s"
                plan_result = execute_query(query, (user_id,))
                if plan_result:
                    no_of_jobs = plan_result[0]['no_of_jobs']
                    total_jobs = plan_result[0]['total_jobs']
                    assisted_jobs_allowed = plan_result[0]['assisted_jobs_allowed']
                    assisted_jobs_used = plan_result[0]['assisted_jobs_used']

                    assisted_jobs_left = assisted_jobs_allowed - assisted_jobs_used
                    if assisted_jobs_left < 0:
                        assisted_jobs_left = 0

                if qry_result:
                    current_period_end = qry_result[0]['current_period_end']
                    converted_date = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
                    rem_days = converted_date - datetime.now(timezone.utc)
                    remaining_days = rem_days.days
                    country = qry_result[0]["country"]
                    pricing_category = qry_result[0]["pricing_category"]     
                    payment_status = qry_result[0]["payment_status"] 
                    is_trial_started = qry_result[0]["is_trial_started"]
                    is_cancelled = qry_result[0]["is_cancelled"]
                    payment_currency = qry_result[0]['payment_currency']

                    return api_json_response_format(True,"success",0,{"payment_status":payment_status,"product_plan":pricing_category, "country" : country, "is_trial_started" : is_trial_started, "is_cancelled" : is_cancelled, "payment_currency" : payment_currency, "remaining_days" : remaining_days, "email_id" : user_email_id, "from_stripe" : from_stripe, "years_of_subcription" : validity_year, "no_of_jobs": no_of_jobs, "assisted_jobs_left": assisted_jobs_left})           
                else:
                    return api_json_response_format(False,"We notice that you're currently not subscribed our product. Please choose a subscription plan suites for you",403,{"country" : country})                             
            else:
                country = 'USA'
                return api_json_response_format(False,"Invalid Token. Please try again",401,{"country" : country})
        except Exception as ex:
            country = 'USA'                
            print("Error in getting checkout status. Error : "+str(ex))
            return api_json_response_format(False,"We notice that you're currently not subscribed our product. Please choose a subscription plan suites for you",403,{"country" : country})

import razorpay
import hmac
import hashlib

RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")       #os.getenv("RAZORPAY_TEST_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")    #os.getenv("RAZORPAY_TEST_KEY_SECRET")
# YOUR_DOMAIN = 'http://localhost:5173'
client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

# def get_customer_token():
#     try:
#         request_data = request.get_json()
#         email_id = request_data.get('email_id')
#         query = 'select customer_id from razorpay_customers where email_id = %s'
#         values = (email_id,)
#         result = execute_query(query, values)
#         if result:
#             customers = client.customer.all()
#             for customer in customers['items']:
#                 if customer['email'] == 'abc@example.com':
#                     print("Customer ID:", customer['id'])
#                     customer_id = customer['id']
#             # customer_id = result[0]['customer_id']
#             # if not customer_id:
#             #     return jsonify({"error": "Customer ID not found"}), 404
            
#             # Fetch tokens for the customer
#             # tokens_list = client.fetch_tokens(customer_id)
#             tokens_list = client.token.all(customer_id = customer_id)

#             # tokens_list['items'] is a list of token objects
#             if tokens_list['items']:
#                 for token in tokens_list['items']:
#                     print(f"Token ID: {token['id']}, Method: {token['method']}, Status: {token['status']}")
#                 return jsonify({"tokens": tokens_list['items']})
#             else:
#                 return jsonify({"message": "No active tokens found for this customer."}), 404
#         else:
#             return jsonify({"error": "Customer not found"}), 404
#     except Exception as e:
#         print(f"Error fetching customer tokens: {e}")
#         return jsonify({"error": str(e)}), 500

def razorpay_create_checkout_session(request):
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()

            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'plan' not in req_data:
                result_json = api_json_response_format(False,"Plan is required",204,{})
                return result_json
            
            email_id = str(req_data['email_id'])
            plan = str(req_data['plan'])
            user_data = get_user_data(email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
                user_id = user_data["sub_user_id"]
            else:
                user_id = user_data['user_id']     

            if plan == 'Partner Basic':
                plan = 'basic'

            updated_at = datetime.now(timezone.utc)
            current_period_start = int(updated_at.timestamp())
            current_period_end = current_period_start + (7 * 24 * 60 * 60)
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "partner":
                update_subscription_query = "INSERT INTO razorpay_customers (email_id, subscription_status, pricing_category, payment_status, old_plan, current_period_start, current_period_end, is_trial_started, is_cancelled, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                update_subscription_values = (email_id, 'trialing', plan, 'trialing', 'default', current_period_start, current_period_end, 'Y', 'N', updated_at,)
                update_query(update_subscription_query, update_subscription_values)

                update_pricing_query = 'update users set payment_status = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s  where email_id = %s'
                update_pricing_values = ('trialing', plan, 'default', 'Y', current_period_start, current_period_end, 'N', email_id,)
                update_query(update_pricing_query, update_pricing_values)
                if user_data['user_role'] != 'partner':
                    update_sub_users = 'update sub_users set payment_status = %s, pricing_category = %s, old_plan = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s  where user_id = %s'
                    sub_users_values = ('trialing', plan, 'default', 'Y', current_period_start, current_period_end, 'N', user_id,)
                    update_query(update_sub_users, sub_users_values)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
                return result_json

            if user_data['user_role'] == 'employer' or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                background_runner.get_employer_details(user_id)
                notification_msg = "Thank you for choosing our job portal. We’re excited to have you on board and look forward to supporting your hiring journey."
                result_json = api_json_response_format(True,"Free trial activated successfully",200,{'email_id':email_id,'plan':plan,'url' : WEB_APP_URI + '/employer_dashboard/home'})
            elif user_data['user_role'] == 'partner':
                background_runner.get_partner_details(user_id)
                notification_msg = "Thank you for choosing our job portal. We’re excited to have you on board and look forward to supporting your partnership journey."
                result_json = api_json_response_format(True,"Free trial activated successfully",200,{'email_id':email_id,'plan':plan,'url' : WEB_APP_URI + '/partner_dashboard/home'})
            
            created_at = datetime.now(timezone.utc)
            insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
            insert_notification_values = (user_id, notification_msg , created_at,)
            update_query(insert_notification_query,insert_notification_values,)
            
            return result_json
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})  
            return result_json      
    except Exception as e:
        print("Error in checkout_session:", e)
        return api_json_response_format(False,"Something went wrong please try again",500,{})
    finally:
        return result_json
    
def razorpay_upgrade_plan(request):
    try:
        request_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            if 'email_id' not in request_data:
                return api_json_response_format(False,"Email ID is required",204,{})
            if 'plan' not in request_data:
                return api_json_response_format(False,"Plan is required",204,{})
            
            email_id = request_data.get('email_id')
            plan = request_data.get('plan')
            # attribute_plan_name = request_data.get('plan_name')
            # pricing_key = 'plan_QgEz2V73jZMBlX' #'plan_Qz9xsym2Up3Koj'

            user_data = get_user_data(email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
            country_code = user_data['country_code']
            contact_number = user_data['contact_number']
            phone_number = f"+{country_code}{contact_number}" if country_code and contact_number else None
            
            get_pricing_key = "SELECT attribute_value FROM payment_config WHERE plan_name = %s and role = %s"
            values = (plan, 'razorpay',)
            pricing_key_result = execute_query(get_pricing_key, values)
            if pricing_key_result:
                pricing_key = pricing_key_result[0]['attribute_value']
            else:
                pricing_key = ""
                return api_json_response_format(False,"Pricing key not found",204,{})
            
            customers = client.customer.all()
            customer_items = customers.get('items', [])
            email_id = email_id.strip().lower()
            existing_customers = [c for c in customer_items if c['email'] == email_id]
            
            if not existing_customers:
                customer = client.customer.create({"email": email_id, "notes": {'user_id': email_id}})
                customer_id = customer['id']
                if customer and customer_id:
                    insert_customer_query = "INSERT INTO razorpay_customers (email_id, customer_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE customer_id = %s;"
                    insert_customer_values = (email_id, customer_id, customer_id,)
                    update_query(insert_customer_query, insert_customer_values)
                else:
                    return api_json_response_format(False,"Customer creation failed",500,{})
            else:
                customer_id = existing_customers[0]['id']
            
            check_trial_status = "SELECT is_trial_started FROM razorpay_customers WHERE email_id = %s"
            check_trial_values = (email_id,)
            trial_status_result = execute_query(check_trial_status, check_trial_values)
            is_trial_started = trial_status_result[0]['is_trial_started'] if trial_status_result else 'N'
            is_trial_started = 'Yes' if is_trial_started == 'Y' else 'No'
            new_user_flag = "yes" if user_data['pricing_category'] == None else "no"
            existing_plan = user_data['pricing_category'] if user_data['pricing_category'] else "default"
            existing_pricing_key = user_data['existing_pricing_key'] if user_data['existing_pricing_key'] else "NA"
            expire_by = int(time.time()) + (2 * 86400) 

            plan_object = client.plan.fetch(pricing_key)
            current_plan_amount = plan_object['item']['amount'] 
            # if current_plan_amount > 0:
            #     payment_order = client.order.create({
            #                                 'amount': current_plan_amount,               # in paise
            #                                 'currency': 'INR',
            #                                 'notes': {'user_id': email_id, 'plan' : plan, 'type' : 'subscription', 'existing_plan': existing_plan, 'pricing_key' : pricing_key,
            #                                         'existing_pricing_key': pricing_key, 'from_trial' : is_trial_started, 'new_user': new_user_flag}
            #                             })            
            #         # Option 1: Razorpay Payment Page URL for the proration charge
            #     payment_link = client.payment_link.create({
            #                             "amount": current_plan_amount,
            #                             "currency": 'INR',
            #                             "accept_partial": False,
            #                             "description": "Subscribing for a plan",
            #                             "customer": {
            #                                 "name": user_data['first_name'] + " " + user_data['last_name'],    #user_data['first_name'] + " " + user_data['last_name']
            #                                 "email": email_id,
            #                             },
            #                             "notify": {"email": True, "sms": True},
            #                             "reference_id": payment_order['id'],
            #                             'notes': {'user_id': email_id, 'plan' : plan, 'type' : 'subscription', 'existing_plan': existing_plan, 'pricing_key' : pricing_key,
            #                                     'existing_pricing_key': pricing_key, 'from_trial' : is_trial_started, 'new_user': new_user_flag},
            #                             "callback_url": WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+email_id,
            #                             "callback_method": "get"
            #                             })
            #     data = {"url" : payment_link['short_url']}
            #     message = 'Success'
            #     return api_json_response_format(True, f"Successfully subscribed to the {plan} plan", 200, data)
            # else:
            #     data = {"url" : WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + email_id,}
            #     message = "Failure"
            
            # return api_json_response_format("True", message, 200, data) 
            if user_data['user_role'] == 'partner':
                if plan == 'Partner Basic':
                    plan = 'Basic'
                subscription_data = client.subscription.create({
                        'plan_id': pricing_key,
                        'total_count': 12,
                        'quantity': 1,
                        'customer_notify': True,
                        'notes': {'user_id': email_id, 'plan' : plan, 'type' : 'subscription', 'existing_plan': existing_plan, 'pricing_key' : pricing_key,
                                'existing_pricing_key': pricing_key, 'from_trial' : is_trial_started, 'new_user': new_user_flag},                
                        'notify_info': {'notify_phone': phone_number,
                                        'notify_email': email_id}
                        })
                if subscription_data:
                        subscription_id = subscription_data['id']
                        subscription_status = subscription_data['status']
                        existing_plan = subscription_data['notes']['existing_plan']
                        existing_pricing_key = subscription_data['notes']['existing_pricing_key']
                        created_at = datetime.now()
                        insert_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, updated_at = %s where email_id = %s"
                        insert_subscription_values = (subscription_id, pricing_key, plan, subscription_status, existing_plan, existing_pricing_key, created_at, email_id)
                        update_query(insert_subscription_query, insert_subscription_values)
                        print(f"Subscription Created: {subscription_id}")    
                        checkout_url = subscription_data['short_url']
                        return api_json_response_format(True,"success",0,subscription_data)
                else:
                    print("Failed to create subscription in upgrade_plan()")
                    return api_json_response_format(False,"Something went wrong please try again.",500,{})
            elif user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin' or user_data['user_role'] == 'recruiter':
                if current_plan_amount > 0:
                    subscription_data = client.subscription.create({
                        'plan_id': pricing_key,
                        'total_count': 12,
                        'quantity': 1,
                        'customer_notify': True,
                        'notes': {'user_id': email_id, 'plan' : plan, 'type' : 'subscription', 'existing_plan': existing_plan, 'pricing_key' : pricing_key,
                                'existing_pricing_key': pricing_key, 'from_trial' : is_trial_started, 'new_user': new_user_flag},                
                        'notify_info': {'notify_phone': phone_number,
                                        'notify_email': email_id}
                        })
                    if subscription_data:
                        subscription_id = subscription_data['id']
                        subscription_status = subscription_data['status']
                        existing_plan = subscription_data['notes']['existing_plan']
                        existing_pricing_key = subscription_data['notes']['existing_pricing_key']
                        created_at = datetime.now()
                        insert_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, updated_at = %s where email_id = %s"
                        insert_subscription_values = (subscription_id, pricing_key, plan, subscription_status, existing_plan, existing_pricing_key, created_at, email_id)
                        update_query(insert_subscription_query, insert_subscription_values)
                        print(f"Subscription Created: {subscription_id}")    
                        checkout_url = subscription_data['short_url']
                        return api_json_response_format(True,"success",0,subscription_data)
                    else:
                        print("Failed to create subscription in upgrade_plan()")
                        return api_json_response_format(False,"Something went wrong please try again.",500,{})
                else:
                    print("Current plan amount is zero in upgrade_plan()")
                    return api_json_response_format(False,"Something went wrong please try again",500,{})
            else:
                return api_json_response_format(False,"Unauthorized User",401,{})
        else:
            return api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as e:
        print("Error in checkout_session:", e)
        return api_json_response_format(False,"Error in checkout_session",500,{"error": str(e)})

#def razorpay_switch_plan(request):
    try:
        print("razorpay switch_plan")
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]
        else:
            return api_json_response_format(False,"Invalid token.",403,{})
        
        request_json = request.get_json()
        email_id = request_json.get('email_id')
        plan_name = request_json['plan']

        user_data = get_user_data(email_id)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(email_id)
            if not user_data['is_exist']:
                return api_json_response_format("False", "User does not exist", 404, {})

        get_existing_plan_query = "SELECT pricing_category, payment_status, subscription_id, pricing_key, current_period_start, current_period_end, payment_currency FROM razorpay_customers WHERE email_id = %s;" 
        get_existing_plan_values = (email_id,)
        existing_pricing_category_dict = execute_query(get_existing_plan_query, get_existing_plan_values)
        payment_status = None
        current_subscription_id = ''
        current_period_start = 0
        current_period_end = 0
        if existing_pricing_category_dict:
            payment_status = existing_pricing_category_dict[0]['payment_status']
            if payment_status == 'active':
                current_subscription_id = existing_pricing_category_dict[0]['subscription_id']
            existing_pricing_category = str(existing_pricing_category_dict[0]['pricing_category'])
            existing_pricing_key = existing_pricing_category_dict[0]['pricing_key']
            current_period_start = existing_pricing_category_dict[0]['current_period_start']
            current_period_end = existing_pricing_category_dict[0]['current_period_end']
        else:
            existing_pricing_category = "default"
            existing_pricing_key = ''
        get_plan_details_query = "SELECT attribute_value, role, plan_name FROM payment_config WHERE plan_name = %s and role = %s;"
        values = (plan_name, 'razorpay',)
        plan_details = execute_query(get_plan_details_query, values)
        
        if len(plan_details) > 0:
            pricing_key = plan_details[0]['attribute_value']
            plan = plan_details[0]['plan_name']
        else:
            return api_json_response_format("False", "Invalid attribute name", 500, {})

        query = "SELECT customer_id FROM razorpay_customers WHERE email_id = %s;"
        values = (email_id,)
        email_id_dict = execute_query(query, values)
        customer_id = ''
        if email_id_dict:
            customer_id = email_id_dict[0]['customer_id']
            print(customer_id)
        else:
            return api_json_response_format("False", "No customer found with the provided email", 404, {})
        
        current_plan = client.plan.fetch(existing_pricing_key)
        new_plan = client.plan.fetch(pricing_key)
        current_plan_amount = current_plan['item']['amount']    # Amount in paise
        new_plan_amount = new_plan['item']['amount']

        is_upgrade = new_plan_amount > current_plan_amount
        if is_upgrade:
            billing_cycle_end = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            billing_cycle_start = datetime.fromtimestamp(current_period_start, tz=timezone.utc)
            total_days = (billing_cycle_end - billing_cycle_start).days
            days_used = (datetime.now(timezone.utc) - billing_cycle_start).days
            daily_cost_current_plan = round(current_plan_amount / total_days, 2)
            amount_used = daily_cost_current_plan * days_used
            amount_left = current_plan_amount - amount_used
            amount_to_pay = new_plan_amount - amount_left
            if amount_to_pay < 0:
                amount_to_pay = 0
            
            proration_order = client.order.create({
                                    'amount': int(amount_to_pay),               # in paise
                                    'currency': current_plan['item']['currency'],
                                    'receipt': f'proration',
                                    'notes': {
                                        "user_id": email_id,
                                        "plan": plan,
                                        "existing_plan": existing_pricing_category,
                                        "type": "subscription-upgrade",
                                        "existing_pricing_key": pricing_key,
                                        "existing_subscription_id": current_subscription_id,
                                        "proration_amount": str(amount_to_pay),
                                        "from_upgrade": "Yes"
                                    }
                                })            
            # Option 1: Razorpay Payment Page URL for the proration charge
            payment_link = client.payment_link.create({
                                    "amount": int(amount_to_pay),
                                    "currency": current_plan['item']['currency'],
                                    "accept_partial": False,
                                    "description": f"Prorated to: {plan}",
                                    "customer": {
                                        "name": user_data['first_name'] + " " + user_data['last_name'],
                                        "email": email_id,
                                    },
                                    "notify": {"email": True, "sms": True},
                                    "reference_id": proration_order['id'],
                                    'notes': {
                                        "user_id": email_id,
                                        "plan": plan,
                                        "existing_plan": existing_pricing_category,
                                        "type": "subscription-upgrade",
                                        "existing_pricing_key": pricing_key,
                                        "existing_subscription_id": current_subscription_id,
                                        "proration_amount": str(amount_to_pay),
                                        "from_upgrade": "Yes"
                                    },
                                    "callback_url": WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+email_id,
                                    "callback_method": "get"
                                })
            data = {"url" : payment_link['short_url']}
            message = "Success"
        else:
            print("Downgrading plan is not implemented.")
            data = {"url" : WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + email_id,}
            message = "Failure"

        return api_json_response_format("True", message, 200, data)    
    except Exception as e:
        print("Error in switch_plan:", e)
        return api_json_response_format("False", "An error occurred in switch_plan()", 500, {})

def razorpay_switch_plan(request):
    try:
        print("razorpay switch_plan")
        token_result = get_user_token(request)                          
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]
        else:
            return api_json_response_format(False,"Invalid token.",403,{})
        
        request_json = request.get_json()
        email_id = request_json.get('email_id')
        plan_name = request_json['plan']

        get_existing_plan_query = "SELECT pricing_category, payment_status, subscription_id, pricing_key, current_period_start, current_period_end, payment_currency FROM razorpay_customers WHERE email_id = %s;" 
        get_existing_plan_values = (email_id,)
        existing_pricing_category_dict = execute_query(get_existing_plan_query, get_existing_plan_values)
        payment_status = None
        current_subscription_id = ''
        current_period_start = 0
        current_period_end = 0
        if existing_pricing_category_dict:
            payment_status = existing_pricing_category_dict[0]['payment_status']
            if payment_status == 'active':
                current_subscription_id = existing_pricing_category_dict[0]['subscription_id']
            existing_pricing_category = str(existing_pricing_category_dict[0]['pricing_category'])
            existing_pricing_key = existing_pricing_category_dict[0]['pricing_key']
            current_period_start = existing_pricing_category_dict[0]['current_period_start']
            current_period_end = existing_pricing_category_dict[0]['current_period_end']
        else:
            existing_pricing_category = "default"
            existing_pricing_key = ''
        get_plan_details_query = "SELECT attribute_value, role, plan_name FROM payment_config WHERE plan_name = %s and role = %s;"
        values = (plan_name, 'razorpay',)
        plan_details = execute_query(get_plan_details_query, values)
        
        if len(plan_details) > 0:
            pricing_key = plan_details[0]['attribute_value']
            plan = plan_details[0]['plan_name']
        else:
            return api_json_response_format("False", "Invalid attribute name", 500, {})

        query = "SELECT customer_id FROM razorpay_customers WHERE email_id = %s;"
        values = (email_id,)
        email_id_dict = execute_query(query, values)
        customer_id = ''
        if email_id_dict:
            customer_id = email_id_dict[0]['customer_id']
            print(customer_id)
        else:
            return api_json_response_format("False", "No customer found with the provided email", 404, {})
        
        current_plan = client.plan.fetch(existing_pricing_key)
        new_plan = client.plan.fetch(pricing_key)
        current_plan_amount = current_plan['item']['amount']    # Amount in paise
        new_plan_amount = new_plan['item']['amount']
        is_upgrade = new_plan_amount > current_plan_amount
        
        if is_upgrade:
            billing_cycle_end = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            billing_cycle_start = datetime.fromtimestamp(current_period_start, tz=timezone.utc)
            total_days = (billing_cycle_end - billing_cycle_start).days
            days_used = (datetime.now(timezone.utc) - billing_cycle_start).days
            daily_cost_current_plan = round(current_plan_amount / total_days, 2)
            amount_used = daily_cost_current_plan * days_used
            amount_left = current_plan_amount - amount_used
            amount_to_pay = new_plan_amount - amount_left
            if amount_to_pay < 0:
                amount_to_pay = 0

            plan_notes = {"user_id": email_id,
                        "plan": plan,
                        "existing_plan": existing_pricing_category,
                        "type": "subscription-upgrade",
                        "existing_pricing_key": existing_pricing_key,
                        "existing_subscription_id": current_subscription_id,
                        "proration_amount": str(amount_to_pay)
            }
            new_plan = client.plan.create({
                                            "period": "yearly",                        # e.g. "month"
                                            "interval": 1,                             # e.g. 1 for monthly
                                            "item": {
                                                "name": "Prorated Plan",
                                                "description": "Prorated plan",
                                                "amount": amount_to_pay,
                                                "currency": "INR"
                                            },
                                            "notes": plan_notes
                                        })
            new_plan_id = new_plan["id"]
            print(f"New plan created: {new_plan_id}")
            # new_plan_id = "plan_R73iMo8SsR91iM"

            data = {
                "plan_id": new_plan_id,
                "quantity": 1,
                "total_count": 12,
                "notes" : {"user_id": email_id,
                            "plan": plan,
                            "existing_plan": existing_pricing_category,
                            "type": "subscription-upgrade",
                            "existing_pricing_key": pricing_key,
                            "existing_subscription_id": current_subscription_id,
                            "proration_amount": str(amount_to_pay),
                            "from_upgrade": "Yes"
                        }
            }
            subscription = client.subscription.create(data)
            subscription_id = subscription['id']
            print(f"Subscription created: {subscription_id}")
            data = subscription
            message = "Success"
            data.update({"url": WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id=' + email_id})
        else:
            print("Downgrading plan is not implemented.")
            data = {"url" : WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + email_id,}
            message = "Failure"
        return api_json_response_format("True", message, 0, data)    
    except Exception as e:
        print("Error in switch_plan:", e)
        return api_json_response_format("False", "An error occurred in switch_plan()", 500, {})
    
def razorpay_create_payment_session(request):
    try:
        token_result = get_user_token(request)                          
        # token_result = {"status_code": 200}      
        # user_email_id = 'test@gmail.com'     
        # user_data = {'first_name' : 'Test', 'last_name' : 'User'}  # Simulating user data for testing
        if token_result["status_code"] == 200:  
            user_email_id = token_result["email_id"]  
            user_data = get_user_data(user_email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
                if user_data["is_exist"]:
                    query = 'select u.email_id from users u LEFT JOIN sub_users su on u.user_id = su.user_id where su.email_id = %s'
                    values = (user_email_id,)
                    email_id_dict = execute_query(query, values)
                    if email_id_dict:
                        user_email_id = email_id_dict[0]['email_id']
            # Get the data from the request
            data = request.get_json()
            if 'lookup_key' not in data:
                return api_json_response_format(False, "Lookup key is required.", 204, {})
            lookup_key = data.get('lookup_key')
            quantity = data.get('no_of_jobs', 1)

            if lookup_key:
                get_price_id = 'select attribute_value from payment_config where attribute_name = %s'
                values = (lookup_key,)
                amount_to_pay_list = execute_query(get_price_id, values)
                if amount_to_pay_list:
                    db_amount = amount_to_pay_list[0]['attribute_value'].strip()
                    # amount_to_pay = int(amount_to_pay_list[0]['attribute_value'])
                else:
                    # amount_to_pay = 15000     # Default amount in paise (15,000 INR)
                    return api_json_response_format(False, "Invalid lookup key.", 204, {})
                amount_to_pay = int(db_amount)
                amount_to_pay *= 100
                quantity = int(quantity)
                amount_to_pay *= quantity
                # amount_to_pay = int(amount_to_pay) * 100
                # amount_to_pay = amount_to_pay * quantity
                proration_order = client.order.create({
                                        'amount': amount_to_pay,               # in paise
                                        'currency': 'INR',
                                        'notes': {
                                            "quantity": quantity,
                                            "user_id": user_email_id,
                                            "type": "additional_jobs"
                                        }
                                    })            
                # Option 1: Razorpay Payment Page URL for the proration charge
                payment_link = client.payment_link.create({
                                        "amount": amount_to_pay,
                                        "currency": 'INR',
                                        "accept_partial": False,
                                        "description": "Additional Jobs",
                                        "customer": {
                                            "name": user_data['first_name'] + " " + user_data['last_name'],    #user_data['first_name'] + " " + user_data['last_name']
                                            "email": user_email_id,
                                        },
                                        "notify": {"email": True, "sms": True},
                                        "reference_id": proration_order['id'],
                                        'notes': {
                                            "quantity": quantity,
                                            "user_id": user_email_id,
                                            "type": "additional_jobs"
                                        },
                                        "callback_url": WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id='+user_email_id,
                                        "callback_method": "get"
                                        })
                data = {"url" : payment_link['short_url']}
                return api_json_response_format(True, "Additional jobs added", 0, data)
            else:
                data = {"url" : WEB_APP_URI + '/checkout_failure?flag=failure&from=partner&session_id={CHECKOUT_SESSION_ID}&user_id=' + user_email_id,}
                return api_json_response_format(False, "Lookup key is required.", 204, data)
        else:
            return api_json_response_format(False,"Invalid token. Please try again.",401,{}) 
    except Exception as e:
        try:
            temp_dict = {'Exception' : str(e),
                        'Message': "Error in creating payment session."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Creating Payment Session Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Creating Payment Session Error",event_properties, temp_dict.get('Message'), user_data=None)
        except Exception as error:  
            print(f"Error in mixpanel event logging: Creating Payment Session Error, {str(error)}")
        print("Initialize payment error : "+str(e))
        return api_json_response_format(False,"Sorry, we are unable to process the payment. We request you to try again.",403,{}) 

def create_order(request):
    try:
        data = request.get_json()  # if you want to take dynamic amount from frontend
        amount = data.get("amount", 50000)  # default 500 INR
        receipt = "receipt_" + datetime.today().strftime("%d%m%Y")
        order_data = {
            "amount": amount,        # Amount in paise
            "currency": "INR",
            "receipt": receipt,
        }
        order = client.order.create(order_data)
        return jsonify(order)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# def capture_payment():
#     try:
#         data = request.get_json()
#         payment_id = data["payment_id"]
#         amount = data["amount"]  # Amount in paise

#         response = client.payment.capture(payment_id, amount)
#         return jsonify(response)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


def verify_signature(payload, signature, secret):
    try:
        generated_signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(generated_signature, signature)
    except Exception as e:
        print(f"Error verifying signature: {e}")
        return False

def razorpay_webhook():
    try:
        payload = request.data.decode('utf-8')
        received_signature = request.headers.get("X-Razorpay-Signature")
        print("razorpay_webhook() called")
        
        if not received_signature or not verify_signature(payload, received_signature, RAZORPAY_WEBHOOK_SECRET):
            return jsonify({"error": "Invalid signature"}), 400

        event_data = json.loads(payload)
        # print(f"EVENT DATA: {event_data}")
        event_type = event_data.get("event")
        if event_type == 'order.paid':
            data = event_data["payload"]["payment"]["entity"]
            notes = data.get('notes',{})
            if notes:
                if notes.get('type') == 'additional_jobs':
                    customer_email_id = notes.get('user_id', 'Unknown')
                    quantity = notes.get('quantity', 0)
                    updated_at = datetime.now()
                else:
                    pricing_category = notes.get('plan', 'Unknown')
                    old_plan = notes.get("existing_plan", None)
                    customer_email_id = notes.get('user_id', 'Unknown')
                    pricing_key = notes.get('existing_pricing_key', 'Unknown')
                    subscription_id = notes.get('existing_subscription_id', 'Unknown')
                    created_at = data.get("created_at", 0)
                    ends_at = created_at + 365 * 24 * 60 * 60  # Assuming a yearly subscription
                    updated_at = datetime.now()
                    payment_method = data.get("method", "Unknown")
                    user_data = get_user_data(customer_email_id)
                    if not user_data['is_exist']:
                        user_data = get_sub_user_data(customer_email_id)
        print(f"EVENT TYPE: {event_type}")
        if event_type == "subscription.activated":#  event_type == "subscription.authenticated" or event_type == "subscription.charged" or event_type == "subscription.cancelled":
            subscription = event_data["payload"]["subscription"]["entity"]
            created_at = subscription.get("created_at", 0)
            current_period_start = subscription.get("current_start", 0)
            current_period_end = subscription.get("current_end", 0)
            payment_method = subscription.get("payment_method", "Unknown")
            pricing_key = subscription.get("plan_id", "Unknown")
            subscription_id = subscription.get("id", "Unknown")
            notes = subscription.get("notes", {})
            customer_email_id = notes.get("user_id", "Unknown")
            existing_pricing_key = notes.get("existing_pricing_key", None)
            old_plan = notes.get("existing_plan", None)
            pricing_category = notes.get("plan", "Unknown")
            existing_subscription_id = notes.get("existing_subscription_id", "Unknown")
            from_upgrade = notes.get("from_upgrade", "No")
            updated_at = datetime.now()
            subscription_customer_id = notes.get("customer_id", "Unknown")
            trial_created = event_data.get("created_at", 0)
            trial_end = subscription.get("end_at", 0) 

            user_data = get_user_data(customer_email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(customer_email_id)
        if event_type == "subscription.authenticated":
            subscription = event_data["payload"]["subscription"]["entity"]
            notes = subscription.get("notes", {})
            customer_email_id = notes.get("user_id", "Unknown")
            existing_pricing_key = notes.get("existing_pricing_key", None)
            old_plan = notes.get("existing_plan", None)
            pricing_category = notes.get("plan", "Unknown")
            existing_subscription_id = notes.get("existing_subscription_id", "Unknown")
            from_upgrade = notes.get("from_upgrade", "No")
            updated_at = datetime.now()
            if from_upgrade == "Yes":
                client.subscription.cancel(existing_subscription_id, { "cancel_at_cycle_end": False })

        # if event_type == "payment.captured":
        #     data = event_data["payload"]["payment"]["entity"]
        #     notes = data.get('notes',{})
        #     if notes:
        #         if notes.get('type') != 'additional_jobs':
        #             customer_email_id = notes.get('user_id', 'Unknown')
        #             pricing_category = notes.get('plan', 'Unknown')
        #             old_plan = notes.get("existing_plan", None)
        #             pricing_key = notes.get('pricing_key', 'Unknown')
        #             subscription_id = notes.get('existing_subscription_id', 'Unknown')
        #             created_at = data.get("created_at", 0)
        #             ends_at = created_at + 365 * 24 * 60 * 60  # Assuming a yearly subscription
        #             updated_at = datetime.now()
        # if event_type == "subscription.authenticated":
        #     if pricing_category == "free_trial":
        #         user_id = user_data['user_id'] if user_data['is_exist'] else 0
        #         user_role = user_data['user_role'] if user_data['is_exist'] else ''
        #         update_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, payment_status = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, payment_method = %s, updated_at = %s where email_id = %s"
        #         update_subscription_values = (subscription_id, pricing_key, 'trialing', 'trialing', event_type, old_plan, existing_pricing_key, trial_created, trial_end, 'inr', payment_method, updated_at, customer_email_id)
        #         update_query(update_subscription_query, update_subscription_values)
        #         print(f"Razorpay Subscription Authenticated: {subscription['id']} for Customer: {customer_email_id}")

        #         update_pricing_query = 'update users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, is_cancelled = %s  where email_id = %s'
        #         update_pricing_values = ('trialing', subscription_id, 'Basic', old_plan, existing_pricing_key, 'Y', trial_created, trial_end, 'inr', 'N', customer_email_id,)
        #         update_query(update_pricing_query, update_pricing_values)

        #         update_sub_users = 'update sub_users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s,  is_cancelled = %s where user_id = %s'
        #         sub_users_values = ('trialing', subscription_id, 'Basic', old_plan, existing_pricing_key, 'Y', trial_created, trial_end, 'inr', 'N', user_data['user_id'],)
        #         update_query(update_sub_users, sub_users_values)
        #         if user_role == 'employer':
        #             background_runner.get_employer_details(user_id)
        #         if user_role == 'partner':
        #             background_runner.get_partner_details(user_id)
            # elif pricing_category != "free_trial":
            #     insert_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, payment_method = %s, updated_at = %s where email_id = %s"
            #     insert_subscription_values = (subscription_id, pricing_key, pricing_category, event_type, old_plan, existing_pricing_key, trial_created, trial_end, payment_method, updated_at, customer_email_id)
            #     update_query(insert_subscription_query, insert_subscription_values)
            #     print(f"Razorpay Subscription Authenticated: {subscription['id']} for Customer: {customer_email_id}")
        
        if event_type == "order.paid":
            if isinstance(data.get('notes'), dict):
                if notes.get('type') == 'additional_jobs':
                    customer_email_id = notes.get('user_id', 'Unknown')
                    quantity = notes.get('quantity', 0)

                    user_data = get_user_data(customer_email_id)
                    if not user_data['is_exist']:
                        user_data = get_sub_user_data(customer_email_id)
                    
                    if user_data['user_role'] == 'partner':
                        update_user_plan_query = "UPDATE user_plan_details SET total_jobs = total_jobs + %s, no_of_jobs = no_of_jobs + %s, additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                        update_user_plan_values = (int(quantity), int(quantity), int(quantity), user_data['user_id'],)
                        update_query(update_user_plan_query, update_user_plan_values)
                    elif user_data['user_role'] == 'employer':
                        update_user_plan_query = "UPDATE user_plan_details SET additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                        update_user_plan_values = (int(quantity), user_data['user_id'],)
                        update_query(update_user_plan_query, update_user_plan_values)
                    
                    notification_msg = f"You have successfully added an additional post(s) to your account!"
                    insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
                    insert_notification_values = (user_data['user_id'], notification_msg, updated_at)
                    update_query(insert_notification_query, insert_notification_values)
                    print(f"Razorpay Order Paid: {data['id']} for Customer: {customer_email_id} for additional jobs")
                else:
                    user_id = user_data['user_id'] if user_data['is_exist'] else 0
                    user_role = user_data['user_role'] if user_data['is_exist'] else ''
                    update_subscription_query = "update razorpay_customers set pricing_category = %s, pricing_key = %s, payment_status = %s, subscription_status = %s, old_plan = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, payment_method = %s, is_trial_started = %s, updated_at = %s where email_id = %s"
                    update_subscription_values = (pricing_category, pricing_key, 'active', event_type, old_plan, created_at, ends_at, 'inr', payment_method, 'Y', updated_at, customer_email_id)
                    update_query(update_subscription_query, update_subscription_values)

                    update_users_table = 'update users set payment_status = %s, pricing_category = %s, existing_pricing_key = %s, old_plan = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, is_cancelled = %s, is_trial_started = %s where email_id = %s'
                    update_users_values = ('active', pricing_category, pricing_key, old_plan, created_at, ends_at, 'inr', 'N', 'Y', customer_email_id,)
                    update_query(update_users_table, update_users_values)

                    update_sub_users = 'update sub_users set payment_status = %s, pricing_category = %s, existing_pricing_key = %s, old_plan = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, is_cancelled = %s, is_trial_started = %s where user_id = %s'
                    sub_users_values = ('active', pricing_category, pricing_key, old_plan, created_at, ends_at, 'inr', 'N', 'Y', user_id,)
                    update_query(update_sub_users, sub_users_values)

                    if old_plan == 'Unknown' or old_plan == 'default' or old_plan == None:
                        notification_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {pricing_category} plan!"
                    else:
                        notification_msg = f"You have successfully signed up for the {pricing_category} plan!"
                    insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
                    insert_notification_values = (user_id, notification_msg, updated_at)
                    update_query(insert_notification_query, insert_notification_values)
                    job_count = 0
                    if user_role == 'employer':
                        background_runner.get_employer_details(user_id)
                        if pricing_category == 'Basic':
                            job_count = 2
                        elif pricing_category == 'Premium':
                            job_count = 5
                        else:
                            job_count = 10
                    elif user_role == 'partner':
                        background_runner.get_partner_details(user_id)
                        if pricing_category == "Basic":
                            job_count = 4
                    query = "UPDATE user_plan_details SET total_jobs = %s, user_plan = %s, no_of_jobs = %s, created_at = %s WHERE user_id = %s;"
                    values = (job_count, pricing_category, job_count, updated_at, user_id,)
                    update_query(query, values)

        elif event_type == "subscription.activated":
            payment_currency = event_data['payload']['payment']['entity'].get("currency", "Unknown")
            update_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, payment_status = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, payment_method = %s, updated_at = %s where email_id = %s"
            update_subscription_values = (subscription_id, pricing_key, pricing_category, 'active', event_type, old_plan, existing_pricing_key, current_period_start, current_period_end, 'inr', payment_method, updated_at, customer_email_id)
            update_query(update_subscription_query, update_subscription_values)
            print(f"Razorpay Subscription Activated: {subscription['id']} for Customer: {customer_email_id}")
            
            update_pricing_query = 'update users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, is_cancelled = %s  where email_id = %s'
            update_pricing_values = ('active', subscription_id, pricing_category, old_plan, pricing_key, 'Y', current_period_start, current_period_end, 'inr', 'N', customer_email_id,)
            update_query(update_pricing_query, update_pricing_values)

            update_sub_users = 'update sub_users set payment_status = %s, subscription_id = %s, pricing_category = %s, old_plan = %s, existing_pricing_key = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s,  is_cancelled = %s where user_id = %s'
            sub_users_values = ('active', subscription_id, pricing_category, old_plan, pricing_key, 'Y', current_period_start, current_period_end, 'inr', 'N', user_data['user_id'],)
            update_query(update_sub_users, sub_users_values)

            created_at = datetime.now()
            notification_msg = ''
            old_plan = user_data['pricing_category'] if user_data['is_exist'] else ''
            user_id = user_data['user_id'] if user_data['is_exist'] else 0
            user_role = user_data['user_role'] if user_data['is_exist'] else ''
            if old_plan == 'Unknown' or old_plan == 'default' or old_plan == None:
                notification_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {pricing_category} plan!"
            else:
                notification_msg = f"You have successfully signed up for the {pricing_category} plan!"
            insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
            insert_notification_values = (user_id, notification_msg, updated_at)
            update_query(insert_notification_query, insert_notification_values)
            job_count = 0
            if user_role == 'employer':
                background_runner.get_employer_details(user_id)
                if pricing_category == 'Basic':
                    job_count = 2
                elif pricing_category == 'Premium':
                    job_count = 5
                else:
                    job_count = 10
            elif user_role == 'partner':
                background_runner.get_partner_details(user_id)
                if pricing_category == "Basic":
                    job_count = 4
            query = "UPDATE user_plan_details SET total_jobs = %s, user_plan = %s, no_of_jobs = %s, additional_jobs_count = %s, created_at = %s WHERE user_id = %s;"
            values = (job_count, pricing_category, job_count, 0, updated_at, user_id,)
            update_query(query, values)

        elif event_type == "subscription.cancelled":
            print(f"CANCELLED EVENT DATA: {event_data}")
            subscription = event_data["payload"]["subscription"]["entity"]            
            notes = subscription.get("notes", {})
            customer_email_id = notes.get("email_id", "Unknown")
            updated_at = datetime.now()

            user_data = get_user_data(customer_email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(customer_email_id)

            print(f"Razorpay Subscription Cancelled: {subscription['id']} for Customer: {customer_email_id}")

        # elif event_type == "subscription.completed":
        #     payment_currency = event_data['payload']['payment']['entity'].get("currency", "Unknown")
        #     insert_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, payment_method = %s, updated_at = %s where email_id = %s"
        #     insert_subscription_values = (subscription_id, pricing_key, pricing_category, event_type, old_plan, existing_pricing_key, current_period_start, current_period_end, payment_currency, payment_method, updated_at, customer_email_id)
        #     update_query(insert_subscription_query, insert_subscription_values)
        #     print(f"Razorpay Subscription Completed: {subscription['id']} for Customer: {customer_email_id}")

        
        # elif event_type == "subscription.charged":
        #     payment_currency = event_data['payload']['payment']['entity'].get("currency", "Unknown")
        #     insert_subscription_query = "update razorpay_customers set subscription_id = %s, pricing_key = %s, pricing_category = %s, subscription_status = %s, old_plan = %s, existing_pricing_key = %s, current_period_start = %s, current_period_end = %s, payment_currency = %s, payment_method = %s, updated_at = %s where email_id = %s"
        #     insert_subscription_values = (subscription_id, pricing_key, pricing_category, event_type, old_plan, existing_pricing_key, current_period_start, current_period_end, payment_currency, payment_method, updated_at, customer_email_id)
        #     update_query(insert_subscription_query, insert_subscription_values)
        #     print(f"Payment received for Razorpay Subscription: {subscription['id']} for Customer: {customer_email_id}")
        #     return jsonify({'status': 'success'}), 200

        # if event_type == "payment.failed" or event_type == "payment.pending":
        #     subscription = event_data["payload"]["subscription"]["entity"]
        #     updated_at = datetime.now()
        #     notes = subscription.get("notes", {})
        #     customer_email_id = notes.get("user_id", "Unknown")
        #     insert_subscription_query = "update razorpay_customers set subscription_status = %s, updated_at = %s where email_id = %s"
        #     insert_subscription_values = ('unpaid', updated_at, customer_email_id)
        #     update_query(insert_subscription_query, insert_subscription_values)
        #     print(f"Razorpay {event_type} for Customer: {customer_email_id}")

        # elif event_type == "refund.created":
        #     # subscription = event_data["payload"]["subscription"]["entity"]
        #     print(f"EVENT DATA IN REFUND CREATED: {event_data}")
        #     updated_at = datetime.now()
        #     notes = event_data.get("notes", {})
        #     customer_email_id = notes.get("user_id", "Unknown")
        #     update_subscription_query = "update razorpay_customers set refund_created_status = %s, updated_at = %s where email_id = %s"
        #     update_subscription_values = ('refund_initialized', updated_at, customer_email_id)
        #     update_query(update_subscription_query, update_subscription_values)
        #     print(f"Razorpay {event_type} for Customer: {customer_email_id}")

        # elif event_type == "refund.processed":
        #     # subscription = event_data["payload"]["subscription"]["entity"]
        #     print(f"EVENT DATA IN REFUND PROCESSED: {event_data}")
        #     updated_at = datetime.now()
        #     notes = subscription.get("notes", {})
        #     customer_email_id = notes.get("user_id", "Unknown")
        #     update_subscription_query = "update razorpay_customers set refund_processed_status = %s, updated_at = %s where email_id = %s"
        #     update_subscription_values = ('refund_processed', updated_at, customer_email_id)
        #     update_query(update_subscription_query, update_subscription_values)
        #     print(f"Razorpay {event_type} for Customer: {customer_email_id}")

        # elif event_type == "refund.failed":
        #     print(f"EVENT DATA IN REFUND FAILED: {event_data}")
        #     # subscription = event_data["payload"]["subscription"]["entity"]
        #     updated_at = datetime.now()
        #     notes = subscription.get("notes", {})
        #     customer_email_id = notes.get("user_id", "Unknown")
        #     update_subscription_query = "update razorpay_customers set refund_failed_status = %s, updated_at = %s where email_id = %s"
        #     update_subscription_values = ('refund_failed', updated_at, customer_email_id)
        #     update_query(update_subscription_query, update_subscription_values)
        #     print(f"Razorpay {event_type} for Customer: {customer_email_id}")
        else:
            print(f"Unhandled event: {event_type}")
        # success_url = WEB_APP_URI +'/checkout_success?flag=success&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id=' + customer_email_id,
        # cancel_url = WEB_APP_URI + '/checkout_failure?flag=failure&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id=' +  customer_email_id
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error handling webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500

def cancel_razorpay_subscription():
    try:
        request_data = request.get_json()
        if 'email_id' not in request_data:
            return jsonify({"error": "Email ID is required"}), 400
        
        email_id = request_data.get('email_id')
        user_data = get_user_data(email_id)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(email_id)
        
        get_customer_query = "SELECT subscription_id FROM razorpay_customers WHERE email_id = %s"
        values = (email_id,)
        subscription_result = execute_query(get_customer_query, values)
        subscription_id = ''
        if not subscription_result:
            return api_json_response_format(False, "Customer not found", 404, {})
        else:
            subscription_id = subscription_result[0]['subscription_id']

        # subscriptions = client.subscription.fetch('sub_QRzv2pIFzZ8cfB')
        # if not subscriptions['items']:
        #     return jsonify({"error": "No active subscriptions found for this customer."}), 404
        
        # subscription = subscriptions['items'][0]  # Assuming we cancel the first subscription
        # subscription_id = subscription['id']
        
        # Cancel the subscription
        try:
            updated_at = datetime.now()
            client.subscription.cancel(subscription_id, 
                                       {"cancel_at_cycle_end": True}
                                    )
            update_status_query = "UPDATE razorpay_customers SET subscription_status = %s, is_cancelled = %s, updated_at = %s WHERE email_id = %s"
            update_values = ('subscription.cancelled', 'Y', updated_at , email_id)
            update_query(update_status_query, update_values)

            update_users_table = "UPDATE users SET is_cancelled = %s WHERE email_id = %s"
            update_users_values = ('Y', email_id)
            update_query(update_users_table, update_users_values)

            update_sub_users_table = "UPDATE sub_users SET is_cancelled = %s WHERE user_id = %s"
            update_sub_users_values = ('Y', user_data['user_id'])
            update_query(update_sub_users_table, update_sub_users_values)

            notification_msg = f"You have successfully cancelled your subscription."
            insert_notification_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s, %s, %s)"
            insert_notification_values = (user_data['user_id'], notification_msg, updated_at)
            update_query(insert_notification_query, insert_notification_values)
            background_runner.send_plan_cancelled_email(email_id)
            return api_json_response_format(True, "Hi "+str(email_id)+", You have successfully cancelled your subscription.", 0, {})
        except Exception as e:
            print(f"Error cancelling subscription: {str(e)}")
            return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})
    except Exception as e:
        print(f"Error cancelling subscription: {str(e)}")
        return api_json_response_format(False,"Sorry, we are unable to complete the operation. We request you to try again.",403,{})

# # from chargebee import Chargebee
# import chargebee
# # from chargebee import filters


# try:
#     cb_client = chargebee.configure(site = "2ndcareers-test", api_key = "test_79uaTfZIbuu0fKjxirJfQb8CJjIq5qPG")   #https://2ndcareers-test.chargebee.com
# except Exception as e:
#     print(f"Error configuring Chargebee: {str(e)}")

# def chargebee_checkout():
#     ###################### Creating customer #########################
#     # result = chargebee.Customer.create({
#     #                     "first_name": "Sachin",
#     #                     "last_name": "Mills",
#     #                     "email": "mills.sachin@example.com"
#     #                  })
#     # customer = result.customer
#     # print(f"Customer created: {customer}")

#     ###################### Gateway Configuration #########################

#     # result = chargebee.PaymentSource.create_using_payment_intent({
#     # "customer_id": "AzqBMmUqtUP071Qt",
#     # "payment_intent": {
#     #     "gateway_account_id": "gw_AzyXWKUpmdjTj5Etk" 
#     #     #"gw_token": "razorpay_payment_id"           # From Razorpay.js
#     #     }
#     # })
#     # print(result)


#     ###################### Subscription list #########################
#     response = chargebee.Subscription.list()
#     for entry in response:
#         subscription = entry.subscription
#         print(f"ID: {subscription.id}, Status: {subscription.status}, Plan: {subscription.plan_id}")

#     ###################### Creating hosted page for checkout #########################
    
#     result = chargebee.HostedPage.checkout_new_for_items({
#                                     "subscription_items": [
#                                         {"item_price_id": "cbdemo_professional-suite-annual"}
#                                     ],
#                                     "customer": {
#                                         "id": "AzqBMmUqtUP071Qt"
#                                     }
#                                 })

#     hosted_page = result.hosted_page

#     print(f"Hosted page created: {hosted_page}")


#     return jsonify({"status": "success"}), 200

# def cb_webhook():
#     try:
#         payload = request.data.decode('utf-8')
#         print("cb_webhook() called")
        
#         event_data = json.loads(payload)
#         print(event_data)
#         event_type = event_data.get("event_type")
#         content = event_data.get("content", {})
#         plan = content.get("plan", {})
#         customer = plan.get("name", {})
        
#         # if event_type == "subscription_created":
#         #     customer_id = customer.get("id", "Unknown")
#         #     email_id = customer.get("email", "Unknown")
#         #     subscription_id = subscription.get("id", "Unknown")
#         #     pricing_key = subscription.get("plan_id", "Unknown")
#         #     pricing_category = subscription.get("plan_quantity", "Unknown")
#         #     created_at = subscription.get("created_at", 0)
#         #     current_period_start = subscription.get("current_period_start", 0)
#         #     current_period_end = subscription.get("current_period_end", 0)
            
#         #     insert_subscription_query = """
#         #         INSERT INTO user_subscription_details(
#         #             customer_id, email_id, subscription_id, pricing_key, pricing_category, 
#         #             subscription_status, created_at, current_period_start, current_period_end
#         #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         #     """
#         #     insert_subscription_values = (
#         #         customer_id, email_id, subscription_id, pricing_key,
#         #         pricing_category, event_type, created_at,
#         #         current_period_start, current_period_end
#         #     )
#         #     update_query(insert_subscription_query, insert_subscription_values)
#         #     print(f"Chargebee Subscription Created: {subscription['id']} for Customer: {customer_id}")

#         # elif event_type == "subscription_activated":
#         #     payment_currency = content['payment_source'].get("currency", "Unknown")
            
#         #     insert_subscription_query = """
#         #         INSERT INTO user_subscription_details(
#         #             customer_id, email_id, subscription_id, pricing_key, pricing_category, 
#         #             subscription_status, created_at, current_period_start, current_period_end,
#         #             payment_currency
#         #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
#         #     """
#         #     insert_subscription_values = (
#         #         customer_id, email_id, subscription_id, pricing_key,)
#         return jsonify({"status": "success"}), 200
#     except Exception as e:
#         print(f"Error in Chargebee webhook: {str(e)}")
#         return jsonify({"error": str(e)}), 500
