
from src import app
from src.models.mysql_connector import execute_query, update_query, update_query_last_index
from flask import jsonify, request
import json
import stripe
import razorpay
import os
from datetime import datetime, timezone, timedelta
from base64 import b64encode, b64decode
import base64
from src.controllers.jwt_tokens.jwt_token_required import token_authentication, get_user_token
from src.models.user_authentication import api_json_response_format, get_user_data, get_sub_user_data
import hmac
import hashlib
import time
from flask_executor import Executor
from src.models.background_task_new import NewBackgroundTask
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

from src.controllers.employer.employer_process import transfer_jobs_to_owner


executor = Executor(app)
background_runner = NewBackgroundTask(executor)

# Load environment variables
API_URI = os.environ.get('API_URI')
STRIPE_KEY = os.environ.get('STRIPE_KEY')
WH_SEC_KEY = os.environ.get('WH_SEC_KEY')
WEB_APP_URI = os.environ.get('WEB_APP_URI')

RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")


WH_SEC_KEY = 'whsec_uwbsFe2ICD0MPw9Rreyqy4lw81RzSShe'

stripe.api_key = STRIPE_KEY

try:
    razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    razorpay_client.set_app_details({"title": "2ndCareers", "version": "1.0"})
except Exception as e:
    print(f"[!] Error initializing Razorpay client: {e}")
    razorpay_client = None


@dataclass
class PromotionContext:
    """Context for promotion evaluation"""
    plan_name: str
    transaction_type: str  # signup, renewal, upgrade, downgrade, addon_jobs, assisted_jobs
    user_id: Optional[int] = None
    user_role: Optional[str] = None
    subscription_plan: Optional[int] = None
    subscription_id: Optional[int] = None
    original_amount: float = 0.0
    currency: Optional[str] = None
    coupon_code: Optional[str] = None
    years: int = 1
    assisted_job_count: int = 0
    addon_count: int = 0
    current_plan: Optional[str] = None
    days_until_expiry: Optional[int] = None
    validity_end: Optional[int] = None

@dataclass
class PromotionResult:
    """Result of promotion evaluation"""
    promotion_id: int
    promotion_type: str  # discount, coupon, addon, upgrade
    promotion_name: str
    discount_type: str  # percentage, fixed
    discount_value: float
    discount_amount: float
    message: str
    metadata: Optional[Dict] = None



class PromotionEngine:
    """
    Handles all promotion and discount operations.
    Decoupled from payment processing.
    """

    def calculate_discount(self, original_amount: float, discount_type: str, discount_value: float) -> float:
        """Calculate discount amount based on type and value"""
        if discount_type == 'percentage':
            discount = original_amount * float((discount_value / 100))
        else:
            discount = float(discount_value)
        return min(discount, original_amount)

    def evaluate_coupon(self, context: PromotionContext) -> Optional[PromotionResult]:
        """Evaluate coupon code"""
        query = """
            SELECT *
            FROM coupon_codes
            WHERE code = %s AND is_active = %s
        """
        result = execute_query(query, (context.coupon_code, 1))

        if not result:
            return None

        coupon = result[0]

        # Check role eligibility
        if coupon['applicable_user_roles']:
            roles = json.loads(coupon['applicable_user_roles'])
            if context.user_role not in roles:
                return None

        # Check plan eligibility
        if coupon['applicable_plans']:
            plans = json.loads(coupon['applicable_plans'])
            if context.plan_name not in plans:
                return None

        if not context.transaction_type == coupon['valid_for']:
            return None

        discount_value = coupon['discount_value']
        discount_amount = self.calculate_discount(context.original_amount, coupon['discount_type'], discount_value)

        return PromotionResult(
            promotion_id=coupon['id'],
            promotion_type='coupon',
            promotion_name=f"Coupon: {coupon['code']}",
            discount_type=coupon['discount_type'],
            discount_value=discount_value,
            discount_amount=discount_amount,
            message=f"Coupon '{coupon['code']}' applied",
            metadata={'coupon_id': coupon['id'], 'priority': 200}
        )

    def evaluate_signup_discount(self, context: PromotionContext) -> Optional[PromotionResult]:
        """Evaluate signup discount"""
        query = """
            SELECT pr.*, pt.code AS type_code
            FROM promotion_rules pr
            JOIN promotion_types pt ON pr.promotion_type_id = pt.id
            WHERE pt.code = 'discount'
                AND pr.is_active = TRUE
                AND JSON_CONTAINS(
                    JSON_EXTRACT(pr.conditions, '$.subscription_plans'),
                    JSON_QUOTE(%s)
                )
                AND JSON_UNQUOTE(JSON_EXTRACT(pr.conditions, '$.transaction_types')) = %s
                AND JSON_EXTRACT(pr.conditions, '$.years') = %s
                AND (pr.valid_from IS NULL OR pr.valid_from <= NOW())
                AND (pr.valid_until IS NULL OR pr.valid_until >= NOW())
            ORDER BY pr.priority DESC
            LIMIT 1
        """
        result = execute_query(query, (context.plan_name, context.transaction_type, context.years))

        if not result:
            return None

        rule = result[0]
        discount_amount = self.calculate_discount(context.original_amount, rule['discount_type'], rule['discount_value'])

        return PromotionResult(
            promotion_id=rule['id'],
            promotion_type='signup_discount',
            promotion_name=rule['rule_name'],
            discount_type=rule['discount_type'],
            discount_value=float(rule['discount_value']),
            discount_amount=discount_amount,
            message=f"Signup discount applied: {rule['discount_value']}% off",
            metadata={'rule_id': rule['id'], 'priority': rule['priority']}
        )

    def evaluate_renewal_discount(self, context: PromotionContext) -> Optional[PromotionResult]:
        """Evaluate renewal discount"""
        query = """
            SELECT pr.*, pt.code AS type_code
            FROM promotion_rules pr
            JOIN promotion_types pt ON pr.promotion_type_id = pt.id
            WHERE pt.code = 'discount'
            AND pr.is_active = TRUE
            AND JSON_CONTAINS(pr.conditions->'$.subscription_plans', JSON_QUOTE(%s))
            AND JSON_CONTAINS(pr.conditions->'$.transaction_types', JSON_QUOTE(%s))
            AND (pr.valid_from IS NULL OR pr.valid_from <= NOW())
            AND (pr.valid_until IS NULL OR pr.valid_until >= NOW())
            ORDER BY pr.priority DESC
            LIMIT 1;
        """
        result = execute_query(query, (context.plan_name, context.transaction_type))

        if not result:
            return None

        offer = result[0]
        discount_amount = self.calculate_discount(context.original_amount, offer['discount_type'], offer['discount_value'])

        return PromotionResult(
            promotion_id=offer['id'],
            promotion_type='renewal_discount',
            promotion_name=offer['rule_name'],
            discount_type=offer['discount_type'],
            discount_value=float(offer['discount_value']),
            discount_amount=discount_amount,
            message=f"Renewal discount applied: {offer['discount_value']}% off",
            metadata={'offer_id': offer['id'], 'priority': offer['priority']}
        )

    def evaluate_upgrade_discount(self, context: PromotionContext) -> Optional[PromotionResult]:
        """Evaluate upgrade plan discount"""
        if not context.current_plan:
            return None

        query = """
            SELECT pr.*, pt.code as type_code
            FROM promotion_rules pr
            JOIN promotion_types pt ON pr.promotion_type_id = pt.id
            WHERE pt.code = 'discount'
            AND pr.is_active = TRUE
            AND JSON_CONTAINS(pr.conditions->'$.subscription_plans', JSON_QUOTE(%s))
            AND JSON_CONTAINS(pr.conditions->'$.transaction_types', JSON_QUOTE(%s))
            AND CAST(pr.conditions->'$.years' AS UNSIGNED) = %s
            AND (pr.valid_from IS NULL OR pr.valid_from <= NOW())
            AND (pr.valid_until IS NULL OR pr.valid_until >= NOW())
            ORDER BY pr.priority DESC
            LIMIT 1;
        """
        result = execute_query(query, (context.plan_name, context.transaction_type, context.years))

        if not result:
            return None

        upgrade = result[0]
        discount_amount = self.calculate_discount(float(context.original_amount), upgrade['discount_type'], upgrade['discount_value'])

        return PromotionResult(
            promotion_id=upgrade['id'],
            promotion_type='upgrade_discount',
            promotion_name=f"Upgrade: {context.current_plan} → {context.plan_name}",
            discount_type=upgrade['discount_type'],
            discount_value=float(upgrade['discount_value']),
            discount_amount=discount_amount,
            message=f"Upgrade discount: {upgrade['discount_value']}% off",
            metadata={'upgrade_id': upgrade['id'], 'priority': upgrade['priority']}
        )
    def evaluate_downgrade_discount(self, context: PromotionContext) -> Optional[PromotionResult]:
        """Evaluate upgrade plan discount"""
        if not context.current_plan:
            return None

        query = """
            SELECT pr.*, pt.code as type_code
            FROM promotion_rules pr
            JOIN promotion_types pt ON pr.promotion_type_id = pt.id
            WHERE pt.code = 'discount'
            AND pr.is_active = TRUE
            AND JSON_CONTAINS(pr.conditions->'$.subscription_plans', JSON_QUOTE(%s))
            AND JSON_CONTAINS(pr.conditions->'$.transaction_types', JSON_QUOTE(%s))
            AND CAST(pr.conditions->'$.years' AS UNSIGNED) = %s
            AND (pr.valid_from IS NULL OR pr.valid_from <= NOW())
            AND (pr.valid_until IS NULL OR pr.valid_until >= NOW())
            ORDER BY pr.priority DESC
            LIMIT 1;
        """
        result = execute_query(query, (context.plan_name, context.transaction_type, context.years))

        if not result:
            return None

        downgrade = result[0]
        discount_amount = self.calculate_discount(float(context.original_amount), downgrade['discount_type'], downgrade['discount_value'])

        return PromotionResult(
            promotion_id=downgrade['id'],
            promotion_type='downgrade_discount',
            promotion_name=f"Downgrade: {context.current_plan} → {context.plan_name}",
            discount_type=downgrade['discount_type'],
            discount_value=float(downgrade['discount_value']),
            discount_amount=discount_amount,
            message=f"Downgrade discount: {downgrade['discount_value']}% off",
            metadata={'dongrade_id': downgrade['id'], 'priority': downgrade['priority']}
        )

    def evaluate_addon_discount(self, context: PromotionContext):
        """Evaluate addon discount"""
        query = """
            SELECT pr.*, pt.code as type_code
            FROM promotion_rules pr
            JOIN promotion_types pt ON pr.promotion_type_id = pt.id
            WHERE pt.code = 'discount'
            AND pr.is_active = TRUE
            AND JSON_CONTAINS(pr.conditions->'$.transaction_types', JSON_QUOTE(%s))
            AND JSON_CONTAINS(pr.conditions->'$.subscription_plans', JSON_QUOTE(%s))
            AND (pr.valid_from IS NULL OR pr.valid_from <= NOW())
            AND (pr.valid_until IS NULL OR pr.valid_until >= NOW())
            ORDER BY pr.priority DESC
            LIMIT 1
        """
        result = execute_query(query, (context.transaction_type, context.plan_name))

        if not result:
            return None

        rule = result[0]
        discount_amount = self.calculate_discount(context.original_amount, rule['discount_type'], rule['discount_value'])
        current_time = int(time.time())
        validity_end = context.validity_end or 0

        if current_time > validity_end:
            return None
        
        return PromotionResult(
            promotion_id=rule['id'],
            promotion_type='addon_discount',
            promotion_name=rule['rule_name'],
            discount_type=rule['discount_type'],
            discount_value=float(rule['discount_value']),
            discount_amount=discount_amount,
            message="Add-on discount applied",
            metadata={'rule_id': rule['id'], 'priority': rule['priority']}
        )

    def calculate_all_promotions(self, context: PromotionContext) -> Dict[str, Optional[PromotionResult]]:
        """
        Evaluate all applicable promotions with priority handling.
        Returns dictionary of all discount types.
        """
        results = {
            'signup_discount': None,
            'renewal_discount': None,
            'upgrade_discount': None,
            'addon_discount': None,
            'coupon_discount': None,
            'downgrade_discount': None
        }

        # Step 1: Check if coupon exists (coupon overrides signup/renewal discounts)
        has_coupon = False
        if context.coupon_code:
            coupon_result = self.evaluate_coupon(context)
            if coupon_result:
                results['coupon_discount'] = coupon_result
                has_coupon = True

        if not has_coupon:
            # Step 2: Transaction-specific discount
            if context.transaction_type == 'new_plan_subscription':
                signup_result = self.evaluate_signup_discount(context)
                if signup_result:
                    results['signup_discount'] = signup_result

            elif context.transaction_type == 'renewal':
                renewal_result = self.evaluate_renewal_discount(context)
                if renewal_result:
                    results['renewal_discount'] = renewal_result

            elif context.transaction_type == 'upgrade':
                upgrade_result = self.evaluate_upgrade_discount(context)
                if upgrade_result:
                    results['upgrade_discount'] = upgrade_result

            elif context.transaction_type == 'downgrade':
                upgrade_result = self.evaluate_downgrade_discount(context)
                if upgrade_result:
                    results['downgrade_discount'] = upgrade_result

            elif context.transaction_type == 'addon_jobs':
                addon_result = self.evaluate_addon_discount(context)
                if addon_result:
                    results['addon_discount'] = addon_result

        return results
    
    def calculate_addon_job_amount(self, plan_name, currency, addon_job_count):
        """Calculate addon job amount"""
        if addon_job_count <= 0:
            return float(0)
        
        query = """
            SELECT addon_job_price_inr as inr_price, addon_job_price_usd as usd_price
            FROM payment_plans
            WHERE plan = %s
        """
        result = execute_query(query, (plan_name,))
        
        if not result:
            return float(0)
        
        price = float(result[0]['inr_price'] if currency == 'inr' else result[0]['usd_price'])
        return price * addon_job_count
    
    def calculate_assisted_job_amount(self, plan_name, currency,  assisted_job_count):
        """Calculate assisted job amount"""
        if assisted_job_count <= 0:
            return float(0)
        
        query = """
            SELECT assisted_jobs_price_inr as inr_price, assisted_jobs_price_usd as usd_price
            FROM payment_plans
            WHERE plan = %s
        """
        result = execute_query(query, (plan_name,))
        
        if not result:
            return float(0)
        
        price = float(result[0]['inr_price'] if currency == 'inr' else result[0]['usd_price'])
        return price * assisted_job_count
    
    def update_coupon_usage(self, user_id: int, coupon_id: int) -> None:
        """Update coupon usage counters"""
        query = "UPDATE coupon_codes SET current_uses = current_uses + 1 WHERE id = %s"
        update_query(query, (coupon_id,))



class CheckoutStatusManager:
    """
    Manages checkout status retrieval and subscription status checks.
    """

    def get_checkout_status(self, request):
        """Get current checkout/subscription status for authenticated user"""
        try:
            token_result = get_user_token(request)

            if token_result["status_code"] != 200:
                return api_json_response_format(False, "Invalid Token", 401, {"country": "USA"})

            user_email_id = token_result["email_id"]
            user_data = get_user_data(user_email_id)

            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
                if user_data["is_exist"]:
                    query = 'SELECT u.email_id FROM users u LEFT JOIN sub_users su ON u.user_id = su.user_id WHERE su.email_id = %s'
                    email_id_dict = execute_query(query, (user_email_id,))
                    if email_id_dict:
                        user_email_id = email_id_dict[0]['email_id']

            user_id = user_data['user_id']
            
        
            new_subscription_query = """SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"""
            new_subscription_result = execute_query(new_subscription_query, (user_id, 'active'))

            
            old_subscription_query = '''SELECT country, payment_status, pricing_category, is_trial_started, 
                    is_cancelled, payment_currency, current_period_end FROM users WHERE email_id = %s'''
            old_subscription_result = execute_query(old_subscription_query, (user_email_id, ))
            
            if not new_subscription_result and not old_subscription_result: 
                    return api_json_response_format(False, "No subscription found", 403, {"country": "USA"})

            new_data = new_subscription_result[0]
            old_data = old_subscription_result[0]

            current_period_end = new_data['validity_end'] if new_data.get('validity_end') else old_data['current_period_end']
            # current_period_end = qry_result[0]['current_period_end']
            converted_date = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            rem_days = converted_date - datetime.now(timezone.utc)
            remaining_days = rem_days.days

            # return api_json_response_format(True, "Success", 200, {
            #     "payment_status": qry_result[0]["payment_status"],
            #     "product_plan": qry_result[0]["pricing_category"],
            #     "country": qry_result[0]["country"],
            #     "is_trial_started": qry_result[0]["is_trial_started"],
            #     "is_cancelled": qry_result[0]["is_cancelled"],
            #     "payment_currency": qry_result[0]['payment_currency'],
            #     "remaining_days": remaining_days,
            #     "email_id": user_email_id
            # })
            is_canclled = 'Y' if new_data['status'] == 'cancelled' else 'N'

            return api_json_response_format(True, "Success", 200, {
                "payment_status": new_data['status'] if new_data.get('status') else old_data["payment_status"],
                "product_plan": new_data['plan'] if new_data.get('plan') else old_data["pricing_category"],
                "country": new_data['country'] if new_data.get('country') else old_data["country"],
                "is_trial_started": new_data['trial_start'] if new_data.get('trial_start') else old_data["is_trial_started"],
                "is_cancelled": is_canclled,
                "payment_currency": new_data['currency'].lower() if new_data.get('currency') else old_data['payment_currency'].lower(),
                "remaining_days": remaining_days,
                "email_id": user_email_id
            })

        except Exception as ex:
            print(f"[x] Error getting checkout status: {str(ex)}")
            return api_json_response_format(False, "Error retrieving status", 500, {"country": "USA"})




class SubscriptionCancellation:
    """
    Handles subscription cancellation across payment gateways.
    """

    def cancel_subscription(self, user_id: int, gateway: str, email: str, action: str = 'user_requested') -> Dict:
        """Cancel active subscription for user"""
        plan = None
        try:
            query = "SELECT plan as plan_name, status as payment_status, stripe_subscription_id, razorpay_subscription_id FROM subscriptions WHERE user_id = %s AND status = 'active' ORDER BY created_at DESC LIMIT 1"
            result = execute_query(query, (user_id,))
            subscription_data = {}

            if not result:
                print(f"[*] No active subscription found for user_id: {user_id}")
                # return {"success": False, "message": "No active subscription found"}
                if gateway.lower() == 'stripe':
                    query = "SELECT subscription_id, pricing_category, payment_status FROM users WHERE user_id = %s "
                    result = execute_query(query, (user_id,))
                    if result:
                        subscription_data['stripe_subscription_id'] = result[0]['subscription_id']
                        subscription_data['plan_name'] = result[0]['pricing_category']
                        subscription_data['payment_status'] = result[0]['payment_status']
                    else:
                        print("[*] No active subscription found in users table")
                        return {"success": False, "message": "No active subscription found"}
                elif gateway.lower() == 'razorpay':
                    query = "SELECT subscription_id, pricing_category, payment_status payment_status FROM razorpay_customers WHERE email_id = %s"
                    result = execute_query(query, (email,))
                    if not result:
                        return {"success": False, "message": "No active subscription found"}
                    else:
                        print(f"[*] Razorpay subscription found in users table for email: {email}")
                        subscription_data['razorpay_subscription_id'] = result[0]['subscription_id']
                        subscription_data['plan_name'] = result[0]['pricing_category']
                        subscription_data['payment_status'] = result[0]['payment_status']
                        
            else:
                subscription_data = result[0]

            if subscription_data['plan_name'] == "Basic" and subscription_data['payment_status'] == "active":
                   
                    update_query(
                        "UPDATE subscriptions SET status = %s, cancellation_reason = %s, cancelled_at = NOW() WHERE user_id = %s AND status = 'active'",
                        ('cancelled', action, user_id)
                    )

                    update_query(
                        "UPDATE users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                    update_query(
                        "UPDATE sub_users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                   
                    if gateway.lower() == 'razorpay':
                        updated_at = datetime.now()
                        update_status_query = "UPDATE razorpay_customers SET subscription_status = %s, is_cancelled = %s, updated_at = %s WHERE email_id = %s"
                        update_values = ('subscription.cancelled', 'Y', updated_at , email)
                        update_query(update_status_query, update_values)

                    print(f"[*] Basic plan cancellation processed for user_id: {user_id}")
                    return {"success": True, "message": "Basic plan cancellation processed"}
            # Cancel at payment gateway
            if gateway.lower() == 'stripe':
                if action == 'user_requested':
                    # stripe.Subscription.modify(
                        
                    #     subscription_data['stripe_subscription_id'],
                    #     cancel_at_period_end=True,
                        
                    # )
                    stripe.Subscription.modify(
                        subscription_data['stripe_subscription_id'],
                        metadata={"cancelled_by": "user_requested"},
                        cancel_at_period_end=True)

                    # Update database
                    update_query(
                        "UPDATE subscriptions SET status = %s, cancellation_reason = %s, cancelled_at = NOW() WHERE user_id = %s AND status = 'active'",
                        ('cancelled', action, user_id)
                    )
                    

                    update_query(
                        "UPDATE users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                    update_query(
                        "UPDATE sub_users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                    print(f"[*] Stripe subscription cancelled: {subscription_data['stripe_subscription_id']}")
                else:
                    
                    stripe.Subscription.modify(
                        subscription_data['stripe_subscription_id'],
                        metadata={"cancelled_by": action})
                    stripe.Subscription.cancel(
                        subscription_data['stripe_subscription_id'],

                    )
                    # Update database
                    update_query(
                        "UPDATE subscriptions SET status = %s, cancellation_reason = %s, cancelled_at = NOW() WHERE user_id = %s AND status = 'active'",
                        ('cancelled', action, user_id)
                    )
                    is_cancelled = 'Y' if action not in ['upgrade', 'downgrade'] else 'N'

                    update_query(
                        "UPDATE users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('cancelled', is_cancelled, user_id)
                    )

                    update_query(
                        "UPDATE sub_users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('cancelled', is_cancelled, user_id)
                    )

                    print(f"[*] Stripe subscription cancelled: {subscription_data['stripe_subscription_id']}")

            elif gateway.lower() == 'razorpay':

                razorpay_sub_id = subscription_data.get('razorpay_subscription_id')
                is_razorpay_subscription = razorpay_sub_id and razorpay_sub_id.startswith("sub_")
                
                if not is_razorpay_subscription:
                    print("[*] One-time payment detected. Local cancellation only.")

                    update_query(
                        "UPDATE subscriptions SET status=%s, cancellation_reason=%s, cancelled_at=NOW() "
                        "WHERE user_id=%s AND status='active'",
                        ('cancelled', action, user_id)
                    )

                    update_query(
                        "UPDATE users SET is_cancelled=%s WHERE user_id=%s",
                        ('Y', user_id)
                    )

                    update_query(
                        "UPDATE sub_users SET is_cancelled=%s WHERE user_id=%s",
                        ('Y', user_id)
                    )

                    return {"success": True, "message": "Plan cancelled (one-time payment)"}

                
                
                subscription_status = razorpay_client.subscription.fetch(str(subscription_data['razorpay_subscription_id']))
                
                status = subscription_status.get('status')
                
                if action == 'user_requested':

                    if status not in ['active', 'created']:
                        print(f"[*] Razorpay subscription is not active, current status: {status}")
                    else:
                        razorpay_client.subscription.cancel(str(subscription_data['razorpay_subscription_id']))
                        print(f"[*] Razorpay subscription cancelled: {subscription_data['razorpay_subscription_id']}")
                    # Update database
                        # time.sleep(10)

                        print(f"[*] Updating database for user_id: {user_id}, after time.sleep(10)")
                    update_query(
                        "UPDATE subscriptions SET status = %s, cancellation_reason = %s, cancelled_at = NOW() WHERE user_id = %s AND status = 'active'",
                        ('cancelled', action, user_id)
                    )
                    

                    update_query(
                        "UPDATE users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                    update_query(
                        "UPDATE sub_users SET payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                        ('active', 'Y', user_id)
                    )

                else:
                    
                    if status not in ['active', 'created']:
                        print(f"[*] Razorpay subscription is not active, current status: {status}")
                    else:
                        razorpay_client.subscription.cancel(str(subscription_data['razorpay_subscription_id']),{
                            'cancel_at_cycle_end': False
                        })
                    updated_at = datetime.now()
                    update_status_query = "UPDATE razorpay_customers SET subscription_status = %s, is_cancelled = %s, updated_at = %s WHERE email_id = %s"
                    update_values = ('subscription.cancelled', 'Y', updated_at , email)
                    update_query(update_status_query, update_values)

                    update_users_table = "UPDATE users SET is_cancelled = %s WHERE email_id = %s"
                    update_users_values = ('Y', email)
                    update_query(update_users_table, update_users_values)

                    update_sub_users_table = "UPDATE sub_users SET is_cancelled = %s WHERE user_id = %s"
                    update_sub_users_values = ('Y', user_id)
                    update_query(update_sub_users_table, update_sub_users_values)

                print(f"[*] Razorpay subscription cancelled: {subscription_data['razorpay_subscription_id']}")

                   
            else:
                print(f"[x] Unsupported payment gateway: {gateway}")
                return {"success": True, "message": "Unsupported payment gateway"}
            
           
            if action == 'user_requested':
                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                created_at = datetime.now()                    
                values = (user_id,"You have successfully cancelled your subscription.",created_at,)
                update_query(query,values)
                background_runner.send_plan_cancelled_email(email)

            # time.sleep(5)

            return api_json_response_format(True,"Hi "+str(email)+", You have successfully cancelled your subscription.",0,{})

        except Exception as e:
            print(f"[x] Error cancelling subscription: {str(e)}")
            print(f"[x] Cancellation failed for user_id: {user_id}, gateway: {gateway}")
            import traceback
            traceback.print_exc()

            # time.sleep(5)
            return {"success": True, "message": str(e)}




class StripePaymentProcessor:
    """
    Handles all Stripe-specific payment operations.
    Promotion engine is injected as dependency.
    """

    def __init__(self, promotion_engine: PromotionEngine):
        self.promotion_engine = promotion_engine

    def get_or_create_customer(self, email: str, user_id: int) -> str:
        """Get existing or create new Stripe customer"""
        customer = stripe.Customer.list(email=email).data

        if customer:
            return customer[0]['id']

        customer = stripe.Customer.create(email=email, metadata={'user_id': str(user_id)})
        update_query(
            "INSERT INTO user_payment_profiles (user_id, email_id, customer_id, gateway) VALUES (%s, %s, %s, %s)",
            (user_id, email, customer['id'], 'stripe')
        )
        insert_customer_query = "INSERT INTO stripe_customers (email, customer_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE customer_id = %s;"
        insert_customer_values = (email, customer['id'], customer['id'])
        update_query(insert_customer_query, insert_customer_values)

        return customer['id']

    def store_payment_plan_key(self, plan_name: str, key_name: str, key_value: str, gateway: str, currency: Optional[str] = None) -> None:
        """Store payment plan keys for reuse"""
        query = "INSERT INTO payment_plan_keys (plan, key_name, key_value, gateway, currency) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE key_value = %s"
        update_query(query, (plan_name, key_name, key_value, gateway, currency, key_value))

    def get_or_create_product(self, plan_name: str) -> str:
        """Create product in Stripe"""

        query = "SELECT key_value FROM payment_plan_keys WHERE key_name = %s AND plan = %s AND gateway = %s"
        result = execute_query(query, ('product_id', plan_name, 'stripe'))
        if not result:
            product = stripe.Product.create(
                name=f"{plan_name} Subscription Product",
                description=f"Product for {plan_name} subscription plan"
            )
            self.store_payment_plan_key(plan_name, 'product_id', product['id'], 'stripe')
            return product['id']
        return result[0]['key_value']
    
    def get_or_create_price(self,product_id: str,plan_name: str,currency: str,years: int = 1, amount: Optional[float] = None,assisted_job_count: Optional[int] = None) -> str:
        """
        Creates or reuses a Stripe Price based on plan details
        """

        # Get plan details
        query = "SELECT * FROM payment_plans WHERE plan = %s"
        plan = execute_query(query, (plan_name,))
        if not plan:
            raise Exception(f"Plan '{plan_name}' not found in DB")
        plan = plan[0]

        
        if assisted_job_count and assisted_job_count > 0:
            
            assisted_unit_price = float(
                plan["assisted_jobs_price_inr"] if currency.lower() == "inr"
                else plan["assisted_jobs_price_usd"]
            )

           
            unit_amount = int(assisted_unit_price * 100)

            # Check if addon price already exists
            lookup_query = """
                SELECT key_value FROM payment_plan_keys
                WHERE plan = %s AND key_name = 'assisted_jobs_price' AND currency = %s
            """
            existing = execute_query(lookup_query, (plan_name, currency))
            if existing:
                return existing[0]["key_value"]

            # Create assisted jobs price (non-recurring)
            price = stripe.Price.create(
                currency=currency.lower(),
                unit_amount=unit_amount,
                product=product_id,
            )

            # Save into payment_plan_keys
            self.store_payment_plan_key(
                plan_name,
                "assisted_jobs_price",
                price["id"],
                "stripe",
                currency
            )

            return price["id"]

       
        # Base price key name
        base_key = f"base_price_{years}_years_{currency.lower()}"

        # Check if base price already exists
        lookup_query = """
            SELECT key_value FROM payment_plan_keys
            WHERE plan = %s AND key_name = %s AND currency = %s
        """
        existing_base = execute_query(lookup_query, (plan_name, base_key, currency))
        if existing_base:
            return existing_base[0]["key_value"]

        # Get base price
        base_price = float(
            plan["base_price_inr"] if currency.lower() == "inr"
            else plan["base_price_usd"]
        )

        unit_amount = int(base_price * 100) * years

        # Create subscription price
        price = stripe.Price.create(
            currency=currency.lower(),
            unit_amount=unit_amount,
            recurring={
                "interval": "year",
                "interval_count": years
            },
            product=product_id,
        )

        # Store base price
        self.store_payment_plan_key(
            plan_name,
            base_key,
            price["id"],
            "stripe",
            currency
        )

        return price["id"]



    # def create_coupon(self, coupon_name: str, coupon_type: str, coupon_value: float, currency: str, product_id: Optional[str] = None) -> str:
    #     """Create coupon in Stripe"""

    #     coupon_params = {
    #         'name': coupon_name,
    #         'duration': 'once',  
    #         'currency': currency.lower(),
    #     }
        
    #     if coupon_type == 'percentage':
    #         coupon_params['percent_off'] = float(coupon_value)
    #     else:
    #         coupon_params['amount_off'] = int(float(coupon_value) * 100)
        
    #     if product_id:
    #         coupon_params['applies_to'] = {'products': [product_id]}
        
    #     coupon = stripe.Coupon.create(**coupon_params)
    #     return coupon.id
    def create_coupon(self, coupon_name: str, coupon_type: str, coupon_value: float, currency: str, product_id: Optional[str] = None) -> str:
        """Create or retrieve coupon in Stripe"""
        try:
            # Generate unique coupon ID to avoid duplicates
            coupon_id = f"{coupon_name.lower().replace(' ', '_')}_{int(coupon_value)}_{currency.lower()}"
            
            # Try to retrieve existing coupon first
            try:
                existing_coupon = stripe.Coupon.retrieve(coupon_id)
                print(f"[✓] Using existing coupon: {coupon_id}")
                return existing_coupon.id
            except stripe.error.InvalidRequestError:
                # Coupon doesn't exist, create new one
                pass

            coupon_params = {
                'id': coupon_id,  # Set explicit ID
                'name': coupon_name,
                'duration': 'once',  
                'currency': currency.lower(),
            }
            
            if coupon_type == 'percentage':
                coupon_params['percent_off'] = float(coupon_value)
            else:
                coupon_params['amount_off'] = int(float(coupon_value) * 100)
            
            
            # IMPORTANT: Restrict coupon to specific product
            if product_id:
                coupon_params['applies_to'] = {'products': [product_id]}
                print(f"[✓] Coupon restricted to product: {product_id}")
            else:
                print(f"[!] WARNING: Coupon created without product restriction!")
            
            coupon = stripe.Coupon.create(**coupon_params)
            print(f"[✓] Created coupon: {coupon.id} with applies_to: {coupon.get('applies_to')}")
            return coupon.id
            
        except stripe.error.InvalidRequestError as e:
            print(f"[x] Stripe API Error: {str(e)}")
            raise
        except Exception as e:
            print(f"[x] Error creating coupon: {str(e)}")
            raise


    def create_checkout_session(self, context: PromotionContext, customer_id: str, plan_name: str, amount: float, currency: str, years: int, assisted_job_count: int, metadata: Dict, user_role: str,  user_email: str, payment_method: str = 'subscription') -> Dict:
       
        # Success/Cancel URLs
        if context.transaction_type == 'assisted_jobs':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}&assist_job=true"
        elif context.transaction_type == 'addon_jobs':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}&additional_job=true"
        else:
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"
        

        cancel_url = f"{WEB_APP_URI}/checkout_failure?flag=failure&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"
        if user_role == 'Partner':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&from=partner&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"

            cancel_url = f"{WEB_APP_URI}/checkout_failure?flag=failure&from=partner&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"
    
        if payment_method == 'subscription':
            return self.create_subscription(
                customer_id, plan_name, currency, years, assisted_job_count, success_url, cancel_url, metadata
            )
        else:
            return self.create_onetime_payment_checkout(
                customer_id, plan_name, amount, currency, metadata, success_url, cancel_url
            )
        
    def create_subscription(self, customer_id: str, plan_name: str, currency: str, years: int, assisted_job_count: int, success_url: str, cancel_url: str, metadata: Dict = None):
        """Create one-time-payment or subscription"""
        try:

            # # Get or create product and pricess
            # query = "SELECT key_value FROM payment_plan_keys WHERE plan = %s AND key_name = %s"

            # result = execute_query(query, (plan_name, 'product_id'))
            product_id = self.get_or_create_product(plan_name)

            # result = execute_query(query, (plan_name, 'assisted_job_product_id'))
            assisted_job_product_id = self.get_or_create_product('Assisted Jobs')

            # result = execute_query(query, (plan_name,'base_price'))
            base_price = self.get_or_create_price(product_id, plan_name, currency, years)

            signup_discount_type = ''
            signup_discount_value = 0
            
            upgrade_discount_type = ''
            upgrade_discount_value = 0

            if metadata and 'promotions_applied' in metadata:
                promotion_details = json.loads(metadata['promotions_applied'])
                if 'signup_discount' in promotion_details and promotion_details['signup_discount']:
                    signup_discount_type = promotion_details['signup_discount']['discount_type']
                    signup_discount_value = promotion_details['signup_discount']['discount_value']
                if 'upgrade_discount' in promotion_details and promotion_details['upgrade_discount']:
                    signup_discount_type = promotion_details['upgrade_discount']['discount_type']
                    signup_discount_value = promotion_details['upgrade_discount']['discount_value']
                if 'downgrade_discount' in promotion_details and promotion_details['downgrade_discount']:
                    signup_discount_type = promotion_details['downgrade_discount']['discount_type']
                    signup_discount_value = promotion_details['downgrade_discount']['discount_value']

            line_items = []

            line_items = [{
                "price": base_price, 
                "quantity": 1
                }]

            tax_rates = []

            
            checkout_params = {
                "mode": "subscription",
                "customer": customer_id,
                "line_items": line_items,
                "success_url": success_url,
                "cancel_url": cancel_url,
                "metadata": metadata,
                "subscription_data": {
                "metadata": {
                    "initial_years": years,
                    "plan_name": plan_name,
                    "currency": currency
                }
    }
            }

            if years == 1 or years == '1':
                checkout_params['allow_promotion_codes'] = True

            signup_coupon_id = ''
            if signup_discount_value > 0:
                signup_coupon_id = self.create_coupon(f"{plan_name} signup discount", signup_discount_type, signup_discount_value, currency, product_id)
            
            upgrade_coupon_id = ''
            if upgrade_discount_value > 0:
                upgrade_coupon_id = self.create_coupon(f"{plan_name} upgrade discount", upgrade_discount_type, upgrade_discount_value, currency, product_id)
               
            
            if signup_discount_value > 0 and signup_discount_value is not None:
                checkout_params["discounts"] = [{"coupon": signup_coupon_id}]

            if upgrade_discount_value > 0 and upgrade_discount_value is not None:
                checkout_params["discounts"] = [{"coupon": upgrade_coupon_id}]

            if assisted_job_count > 0:

                # Get addon price
                # result = execute_query(query, (plan_name, "assisted_job_price"))
                addon_price_id = (
                   self.get_or_create_price(assisted_job_product_id, plan_name, currency, 1, assisted_job_count=assisted_job_count)
                )

                line_items.append({
                "price": addon_price_id,
                "quantity": assisted_job_count,
            })

            if currency.lower() == 'inr':
                # Get or create GST 18% tax rate
                try:
                    # List all tax rates and find GST 18%
                    all_tax_rates = stripe.TaxRate.list(limit=100)
                    gst_rate_id = None
                    
                    for rate in all_tax_rates.data:
                        if rate.percentage == 18 and rate.jurisdiction == 'IN':
                            gst_rate_id = rate.id
                            print(f"[*] Using existing GST 18% tax rate: {gst_rate_id}")
                            break
                    
                    # Create new tax rate if not found
                    if not gst_rate_id:
                        gst_rate = stripe.TaxRate.create(
                            percentage=18,
                            inclusive=False,  
                            jurisdiction='IN',
                            display_name='GST 18% - India'  
                        )
                        gst_rate_id = gst_rate.id
                        print(f"[*] Created new GST 18% tax rate: {gst_rate_id}")
                    
                    tax_rates = [gst_rate_id]

                    line_items[0]["tax_rates"] = tax_rates
                    if line_items and len(line_items) > 1:
                        line_items[1]["tax_rates"] = tax_rates

                except Exception as e:
                    print(f"[!] Warning: Could not apply GST: {str(e)}")

            
            
            
            checkout_session = stripe.checkout.Session.create(**checkout_params)

            return api_json_response_format(True, "Success", 200, {"url": checkout_session.url})

        except Exception as e:
            print(f"[x] Error creating dynamic subscription schedule: {str(e)}")
            return api_json_response_format(False, str(e), 500, {})
        

    # def create_subscription_schedule(
    #     self,
    #     customer_id: str,
    #     plan_name: str,
    #     currency: str,
    #     years: int,
    #     assisted_job_count: int,
    #     success_url: str,
    #     cancel_url: str,
    #     metadata: Dict = None
    # ) -> Dict:
        
        try:
            # Get or create product and prices
            query = "SELECT key_value FROM payment_plan_keys WHERE plan = %s AND key_name = %s"

            result = execute_query(query, (plan_name, 'product_id'))
            product_id = result[0]['key_value'] if result else self.get_or_create_product(plan_name)

            result = execute_query(query, (plan_name, f"{years}_years_price"))
            initial_price_id = result[0]['key_value'] if result else self.get_or_create_price(product_id, plan_name, currency, years)

            result = execute_query(query, (plan_name, "1_years_price"))
            one_year_price_id = result[0]['key_value'] if result else self.get_or_create_price(product_id, plan_name, currency, 1)

            initial_years = years
            renewal_years = 1
            initial_discount_type = "percentage"
            initial_discount_value = 0
            renewal_discount_type = "percentage"
            renewal_discount_value = 0
            
            if metadata and 'promotions_applied' in metadata:
                promotion_details = json.loads(metadata['promotions_applied'])
                if 'signup_discount' in promotion_details and promotion_details['signup_discount']:
                    initial_discount_type = promotion_details['signup_discount']['discount_type']
                    initial_discount_value = promotion_details['signup_discount']['discount_value']
                if 'renewal_discount' in promotion_details and promotion_details['renewal_discount']:
                    renewal_discount_type = promotion_details['renewal_discount']['discount_type']
                    renewal_discount_value = promotion_details['renewal_discount']['discount_value']
                

            phases = []
            
            # Initialize coupon IDs
            initial_coupon_id = None
            
            phase1 = {
                "items": [{"price": initial_price_id, "quantity": 1}],
                "duration": {
                    "interval": "year",
                    "interval_count": years  
                },
                "metadata": {
                    "phase": f"initial_{years}_years",
                    f"discount_{initial_discount_type}": initial_discount_value,
                    "is_discounted": True
                }
            }
            
            # Apply initial discount if applicable
            if initial_discount_value > 0:
                initial_coupon_id = self.create_coupon(f"{plan_name} initial discount", initial_discount_type, initial_discount_value, currency)
                phase1["discounts"] = [{"coupon": initial_coupon_id}]
                print(f"[*] Phase 1: Applied {initial_discount_value} discount for {initial_years} years")
            
            # phases.append(phase1)
            
           
            phase2 = {
                "items": [{"price": one_year_price_id, "quantity": 1}],
                "duration": {
                    "interval": "year",
                    "interval_count": renewal_years  
                },
                "metadata": {
                    "phase": f"renewal_{renewal_years}_year_first",
                    f"discount_{renewal_discount_type}": renewal_discount_value,
                    "is_discounted": True
                }
            }
            # Apply renewal discount if applicable
            renewal_coupon_id = None
            if renewal_discount_value > 0:
                renewal_coupon_id = self.create_coupon(f"{plan_name} renewal discount", renewal_discount_type, renewal_discount_value, currency)
                phase2["discounts"] = [{"coupon": renewal_coupon_id}]
                print(f"[*] Phase 2: Applied {renewal_discount_value} discount for {renewal_years} year")
            
            phases.append(phase2)
                        
           
            phase3 = {
                "items": [{"price": one_year_price_id, "quantity": 1}],
                "duration": {
                    "interval": "year",
                    "interval_count": renewal_years  
                },
                "metadata": {
                    "phase": f"renewal_{renewal_years}_year_ongoing",
                    "is_discounted": False
                }
            }
            
            
            # Apply ongoing discount if applicable (usually 0)
            # if ongoing_discount_percent > 0:
            #     ongoing_coupon_id = self.create_dynamic_coupon(
            #         f"{plan_name}_ongoing_{ongoing_discount_percent}pct",
            #         discount_type="percentage",
            #         discount_value=ongoing_discount_percent,
            #         currency=currency
            #     )
            #     phase3["discounts"] = [{"coupon": ongoing_coupon_id}]
            
            phases.append(phase3)
            print(phases)
            print(f"[*] Built subscription schedule with {len(phases)} phases")
            
            # Step 3: Create checkout session
            checkout_params = {
                "mode": "subscription",
                "customer": customer_id,
                "line_items": [{"price": one_year_price_id, "quantity": years}],
                "success_url": success_url,
                "cancel_url": cancel_url,
                "metadata": metadata
            }
            
            if initial_discount_value > 0 and initial_coupon_id is not None:
                checkout_params["discounts"] = [{"coupon": initial_coupon_id}]
            
            # if assisted_job_count > 0:
            #     result = execute_query(query, (plan_name, "asssited_job_price"))
            #     assisted_job_price_id = result[0]['key_value'] if result else self.get_or_create_price(product_id, plan_name, currency, 1, assisted_job_count=assisted_job_count)

            #     checkout_params["line_items"].append({"price": assisted_job_price_id, "quantity": assisted_job_count})
            

            checkout_session = stripe.checkout.Session.create(**checkout_params)
            
            

               


            # Step 4: Create subscription schedule from checkout session
            # subscription_schedule = stripe.SubscriptionSchedule.create(
            #     from_subscription=checkout_session.subscription,
            #     customer=customer_id,
            #     start_date='now',
            #     phases=phases,
            #     end_behavior="release"
            # )
            
            # print(f"[*] Subscription schedule created: {subscription_schedule.id}")
            # print(f"    Phase 1: {initial_years} years @ {initial_discount_value}% off")
            # print(f"    Phase 2: {renewal_years} year @ {renewal_discount_value}% off")
            # print(f"    Phase 3: {renewal_years} year @ full price (ongoing)")
            
            # return api_json_response_format(
            #     True, "Success", 200,
            #     {
            #         "url": checkout_session.url,
            #         "schedule_id": subscription_schedule.id,
            #         "phases": len(phases)
            #     }
            # )

            return api_json_response_format(True, "Success", 200, {"url": checkout_session.url})

        except Exception as e:
            print(f"[x] Error creating dynamic subscription schedule: {str(e)}")
            return api_json_response_format(False, str(e), 500, {})

    # def _create_subscription_checkout(self, customer_id: str, plan_name: str, currency: str, 
    #                                  years: int, metadata: Dict, success_url: str, cancel_url: str) -> Dict:
    #     """Create subscription checkout with multi-phase billing"""
    #     # Get or create product and prices
    #     query = "SELECT key_value FROM payment_plan_keys WHERE plan = %s AND key_name = %s"

    #     result = execute_query(query, (plan_name, 'product_id'))
    #     product_id = result[0]['key_value'] if result else self.get_or_create_product(plan_name)

    #     result = execute_query(query, (plan_name, f"{years}_years_price"))
    #     years_price_id = result[0]['key_value'] if result else self.get_or_create_price(product_id, plan_name, currency, years)

    #     result = execute_query(query, (plan_name, "1_years_price"))
    #     one_year_price_id = result[0]['key_value'] if result else self.get_or_create_price(product_id, plan_name, currency, 1)

    #     # Create checkout session
    #     try:
    #         checkout_session = stripe.checkout.Session.create(
    #             mode="subscription",
    #             customer=customer_id,
    #             line_items=[{"price": years_price_id, "quantity": 1}],
    #             success_url=success_url,
    #             cancel_url=cancel_url,
    #         )

    #         return api_json_response_format(True, "Success", 200, {"url": checkout_session.url})

    #     except Exception as e:
    #         print(f"[x] Subscription checkout error: {str(e)}")
    #         return api_json_response_format(False, f"Error: {str(e)}", 500, {})

    def create_onetime_payment_checkout(self, customer_id: str, plan_name: str, amount: float,
                                        currency: str, metadata: Dict, success_url: str, cancel_url: str) -> Dict:
        """Create one-time payment checkout"""
        try:
            checkout_session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                line_items=[{
                    'price_data': {
                        'currency': currency.lower(),
                        'product_data': {'name': plan_name},
                        'unit_amount': int(float(amount) * 100),
                    },
                    'quantity': 1,
                }],
                mode="payment",
                customer=customer_id,
                metadata=metadata,
                success_url=success_url,
                cancel_url=cancel_url,
                allow_promotion_codes = True
            )


            return api_json_response_format(True, "Success", 200, {"url": checkout_session.url})

        except Exception as e:
            print(f"[x] One-time payment checkout error: {str(e)}")
            return api_json_response_format(False, f"Error: {str(e)}", 500, {})




class RazorpayPaymentProcessor:
    """
    Handles all Razorpay-specific payment operations.
    Follows Stripe logic for consistency.
    Promotion engine is injected as dependency.
    """

    def __init__(self, promotion_engine: PromotionEngine):
        self.promotion_engine = promotion_engine
        self.razorpay_client = razorpay_client

    # def get_or_create_customer(self, email: str, user_id: int) -> str:
    #     """Get existing or create new Razorpay customer"""
    #     query = "SELECT * FROM user_payment_profiles WHERE user_id = %s AND gateway = 'razorpay'"
    #     result = execute_query(query, (user_id,))

    #     if result:
    #         return result[0]['customer_id']

    #     customer_data = {"email": email}
    #     customer = self.razorpay_client.customer.create(customer_data)

    #     update_query(
    #         "INSERT INTO user_payment_profiles (user_id, email_id, customer_id, gateway) VALUES (%s, %s, %s, %s)",
    #         (user_id, email, customer['id'], 'razorpay')
    #     )

    #     return customer['id']
    def get_or_create_customer(self, email: str, user_id: int) -> str:
        """Get existing or create new Razorpay customer"""

        customers = razorpay_client.customer.all()
        customer_items = customers.get('items', [])
        email_id = email.strip().lower()
        existing_customers = [c for c in customer_items if c['email'] == email_id]
        
        if not existing_customers:
            customer = razorpay_client.customer.create({"email": email_id, "notes": {'user_id': email_id}})
            customer_id = customer['id']
            if customer and customer_id:
                insert_customer_query = "INSERT INTO razorpay_customers (email_id, customer_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE customer_id = %s;"
                insert_customer_values = (email_id, customer_id, customer_id,)
                update_query(insert_customer_query, insert_customer_values)

                update_query(
                """
                INSERT INTO user_payment_profiles
                (user_id, email_id, customer_id, gateway)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, email, customer_id, 'razorpay')
            )
            

            return customer_id

        return existing_customers[0]['id']

    def store_payment_plan_key(self, plan_name: str, key_name: str, key_value: str, gateway: str, currency: str) -> None:
        """Store payment plan keys for reuse (same as Stripe)"""
        query = "INSERT INTO payment_plan_keys (plan, key_name, key_value, gateway, currency) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE key_value = %s"
        update_query(query, (plan_name, key_name, key_value, gateway, currency, key_value))

    # def create_plan(self, plan_name: str, currency: str, years: int, amount: Optional[float] = None) -> str:
    #     """Create subscription plan in Razorpay (equivalent to Stripe product + price)"""

    #     query = "SELECT key_value FROM payment_plan_keys WHERE plan = %s AND key_name = %s AND gateway = %s AND currency = %s"
    #     result = execute_query(query, (plan_name, f"{years}_years_plan", "razorpay", currency))
    #     if result:
    #         return result[0]['key_value']
        
    #     if amount is None:
    #         query = "SELECT * FROM payment_plans WHERE plan = %s"
    #         result = execute_query(query, (plan_name,))
    #         amount = float(result[0]['base_price_inr'] if currency == 'inr' else result[0]['base_price_usd'])

    #     amount *= years

    #     # Razorpay expects amount in paise/cents
    #     amount_in_paise = int(amount * 100)

    #     plan_data = {
    #         "period": "yearly",
    #         "interval": years,
    #         "item": {
    #             "name": plan_name,
    #             "amount": amount_in_paise,
    #             "currency": currency.upper(),
    #             "description": f"{plan_name} subscription plan"
    #         },
    #         "notes": {
    #             "plan_name": plan_name,
    #             "years": years
    #         }
    #     }

    #     plan = self.razorpay_client.plan.create(plan_data)

    #     self.store_payment_plan_key(plan_name, f"{years}_years_plan", plan['id'], gateway='razorpay', currency=currency)

    #     return plan['id']
    # def create_plan(self, plan_name: str, currency: str, years: int, amount: Optional[float] = None) -> str:
    #     """Create subscription plan in Razorpay """
    #     if amount:
    #         amount_in_paise = int(amount * 100)

    #         plan_data = {
    #             "period": "yearly",
    #             "interval": years,
    #             "item": {
    #                 "name": plan_name,
    #                 "amount": amount_in_paise,
    #                 "currency": currency.upper(),
    #                 "description": f"{plan_name} subscription plan"
    #             },
    #             "notes": {
    #                 "plan_name": plan_name,
    #                 "years": years
    #             }
    #         }

    #         plan = self.razorpay_client.plan.create(plan_data)
    #         self.store_payment_plan_key(plan_name, f"{amount}_currency", plan['id'], gateway='razorpay', currency=currency)

    #         return plan['id']
        
    #     elif amount is None:
    #         query = "SELECT * FROM payment_plans WHERE plan = %s"
    #         result = execute_query(query, (plan_name,))
    #         if result:
    #             amount = float(result[0]['base_price_inr'] if currency == 'inr' else result[0]['base_price_usd'])
    #             amount *= years

    #             amount_in_paise = int(amount * 100)

    #             plan_data = {
    #                 "period": "yearly",
    #                 "interval": years,
    #                 "item": {
    #                     "name": plan_name,
    #                     "amount": amount_in_paise,
    #                     "currency": currency.upper(),
    #                     "description": f"{plan_name} subscription plan"
    #                 },
    #                 "notes": {
    #                     "plan_name": plan_name,
    #                     "years": years
    #                 }
    #             }

    #             plan = self.razorpay_client.plan.create(plan_data)
    #             self.store_payment_plan_key(plan_name, f"{years}_years_plan", plan['id'], gateway='razorpay', currency=currency)

    #             return plan['id']
    #     else:
    #         raise Exception("Amount must be provided to create a Razorpay plan.")

    def get_payment_plan_key(self, plan_name: str, key_name: str, currency: str) -> Optional[str]:
        query = """
            SELECT key_value FROM payment_plan_keys
            WHERE plan = %s AND key_name = %s AND currency = %s AND gateway = 'razorpay'
        """
        rows = execute_query(query, (plan_name, key_name, currency))
        return rows[0]["key_value"] if rows else None

    # def get_or_create_addon(self, plan_name: str, amount: int,
    #                     currency: str, notes: Dict):

    #     # Check existing addons
    #     addons = self.razorpay_client.addon.all()

    #     for a in addons.get("items", []):
    #         if a["amount"] == amount and a["currency"] == currency:
    #             return a

    #     # Create addon
    #     return self.razorpay_client.subscription.createAddon({
    #         "name": f"{plan_name} Addon",
    #         "amount": amount,
    #         "currency": currency,
    #         "notes": notes
    #     })

    def get_or_create_plan(self, plan_name: str, amount: float,
                       interval: str, interval_count: int, currency: str):

        # Search existing plans
        plans = self.razorpay_client.plan.all()

        amount = int(amount * 100)  # amount in paise/cents

        base_key = f"{amount}_{currency}"
        query = """
            SELECT key_value FROM payment_plan_keys
            WHERE plan = %s AND key_name = %s AND currency = %s AND gateway = 'razorpay'
        """
        result = execute_query(query, (plan_name, base_key, currency))
        if result:
            return result[0]['key_value']
        
        for plan in plans.get("items", []):
            if (plan["period"] == interval
                and plan["interval"] == interval_count
                and plan['item']["amount"] == amount
                and plan['item']["currency"] == currency):
                return plan['id']

        # Create new plan
        plan_data = {
            "period": interval,
            "interval": interval_count,
            "item": {
                "name": plan_name,
                "amount": amount,
                "currency": currency.upper(),
                "description": f"{plan_name} subscription plan"
            },
            "notes": {
                "plan_name": plan_name,
                "years": interval_count if interval == "yearly" else 0
            }
        }
        plan_id =  self.razorpay_client.plan.create(plan_data)
        
        # Store plan ID
        self.store_payment_plan_key(plan_name, base_key, plan_id['id'], "razorpay", currency)

        return plan_id['id']

        
    def create_coupon(self, coupon_name: str, coupon_type: str, coupon_value: float, currency: str, duration_in_months: int = 1) -> str:
        """Create coupon in Razorpay (equivalent to Stripe coupon)"""

        coupon_data = {
            "duration": "fixed",
            "duration_in_months": duration_in_months,
            "percent_rate": int(coupon_value) if coupon_type == 'percentage' else None,
            "fixed_amount": int(coupon_value * 100) if coupon_type == 'fixed' else None,
            "max_discount_amount": None,
            "description": coupon_name,
        }

        # Remove None values
        coupon_data = {k: v for k, v in coupon_data.items() if v is not None}

        coupon = self.razorpay_client.coupon.create(coupon_data)
        return coupon['id']

    def create_dynamic_coupon(
        self,
        coupon_name: str,
        discount_type: str,  # 'percentage' or 'fixed'
        discount_value: float,  # 20 for 20% or 1000 for 1000
        currency: str
    ) -> str:
        """
        Create dynamic coupon from discount parameters
        
        Args:
            coupon_name: Unique coupon identifier
            discount_type: 'percentage' or 'fixed'
            discount_value: Discount amount/percentage
            currency: Currency code
        
        Returns:
            Coupon ID created in Stripe
        """
        try:
            if discount_type == "percentage":
                coupon = stripe.Coupon.create(
                    name=coupon_name,
                    percent_off=discount_value,
                    duration="repeating",
                    duration_in_months=999  # Long duration
                )
            else:  # fixed amount
                coupon = stripe.Coupon.create(
                    name=coupon_name,
                    amount_off=int(discount_value * 100),  # Convert to cents
                    currency=currency,
                    duration="repeating",
                    duration_in_months=999
                )
            
            print(f"[*] Created coupon: {coupon.id} ({discount_type}={discount_value})")
            return coupon.id

        except Exception as e:
            print(f"[x] Failed to create coupon: {e}")
            raise


    

    def create_checkout_session(self, context: PromotionContext, customer_id: str, plan_name: str,
                               amount: float, currency: str, years: int, assisted_job_cout: int, metadata: Dict,
                               email_id: str, phone_number: str, payment_method: str = 'one-time', trial_days: int = 0) -> Dict:
        """
        Create Razorpay checkout with same logic as Stripe.
        Razorpay uses payment_links for one-time and subscriptions.
        """
        try:
            customer = self.razorpay_client.customer.fetch(customer_id)
            amount_in_paise = int(amount * 100)

            if payment_method == 'one-time':
                return self.create_onetime_payment(customer, amount, currency, metadata, email_id, user_role=context.user_role, action=context.transaction_type)
            elif payment_method == 'subscription':
                return self.create_subscription(customer_id, plan_name, currency, amount, years, phone_number, email_id, metadata)
                # return self.create_onetime_payment(customer, amount, currency, metadata, email_id)
            else:
                return api_json_response_format(False, "Invalid payment method", 400, {})

        except Exception as e:
            print(f"[x] Razorpay checkout creation failed: {e}")
            return api_json_response_format(False, "Checkout creation failed", 500, {})

    def create_onetime_payment(self, customer: Dict, amount: float, currency: str, metadata: Dict, user_email: str, user_role: str, action: str) -> Dict:
        """Create one-time payment link in Razorpay"""
        
        if action == 'assisted_jobs':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}&assist_job=true"
        elif action == 'addon_jobs':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}&additional_job=true"
        else:
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"

        cancel_url = f"{WEB_APP_URI}/checkout_failure?flag=failure&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"

        if user_role == 'Partner':
            success_url = f"{WEB_APP_URI}/checkout_success?flag=success&from=partner&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"

            cancel_url = f"{WEB_APP_URI}/checkout_failure?flag=failure&from=partner&session_id={{CHECKOUT_SESSION_ID}}&user_id={user_email}"

        amount = int(round(amount)) * 100
        # amount = 50000100
        print(f"[*] Creating Razorpay one-time payment link for amount: {amount} {currency.upper()}")

        
        payment_link = self.razorpay_client.payment_link.create({
            "amount": amount,
            "currency": currency.upper(),
            "accept_partial": False,
            "customer": {"email": customer['email']},
            "notify": {"sms": True, "email": True},
            "reminder_enable": True,
            "notes": metadata,
            "callback_url": success_url,
            "callback_method": "get"
        })

        return api_json_response_format(True, "Payment link created", 200, {"url": payment_link['short_url']})

    # def create_subscription(self, customer: Dict, customer_id: str, plan_name: str, currency: str, years: int, amount: float, metadata: Dict, trial_days: int = 0, user_email: str = '') -> Dict:
    #     """Create subscription in Razorpay"""
        
    #     # Ensure customer exists
    #     try:
    #         self.razorpay_client.customer.fetch(customer_id)
    #     except Exception as e:
    #         # Create customer if doesn't exist
    #         customer_resp = self.razorpay_client.customer.create({
    #             "email": customer.get('email'),
    #             "contact": customer.get('contact')
    #         })
    #         customer_id = customer_resp['id']
        
    #     # Get or create plan
    #     query = "SELECT key_value FROM payment_plan_keys WHERE plan = %s AND key_name = %s"
    #     result = execute_query(query, (plan_name, f"{years}_years_plan"))
    #     plan_id = result[0]['key_value'] if result else self.create_plan(
    #         plan_name, currency, years, amount
    #     )
        
    #     # Calculate start timestamp
    #     if trial_days > 0:
    #         start_at = int(time.time()) + (trial_days * 24 * 60 * 60)
    #     else:
    #         start_at = int(time.time()) + 3600 # Start at 1 hour 
        
    #     # Flatten metadata for notes
    #     notes = {}
    #     for key, value in metadata.items():
    #         notes[key] = str(value) if not isinstance(value, str) else value
    #     expire_by = start_at + (7 * 24 * 60 * 60)
    #     subscription_data = {
    #         "plan_id": plan_id,
    #         "customer_notify": 1,
    #         "quantity": 1,
    #         "customer_id": customer_id,
    #         "start_at": start_at,
    #         "total_count": years,  # FIXED: was hardcoded to 1
    #         "expire_by": expire_by, 
    #         "notes": notes,
    #     }
        
    #     try:
    #         subscription = self.razorpay_client.subscription.create(subscription_data)
    #         # subscription.update({"url": WEB_APP_URI + '/checkout_success?flag=success&session_id={CHECKOUT_SESSION_ID}&user_id=' + user_email})
    #         return api_json_response_format(True, "Subscription created", 200, {
    #             "subscription_id": subscription['id'],
    #             "plan_id": plan_id,
    #             "url": subscription.get('short_url'),
    #             "status": subscription['status'],
    #             "amount": float(metadata.get('total_amount', 0)),
    #             "currency": currency,
    #             "next_billing_at": subscription.get('start_at')  # Include next billing date
    #         })
    #     except Exception as e:
    #         print(f"[x] Razorpay subscription creation failed: {str(e)}")
    #         return api_json_response_format(False, f"Subscription creation failed: {str(e)}", 400, {})

    def create_razorpay_offer(self, discount_type: str, discount_value: float) -> str:
        """Create offer in Razorpay (equivalent to Stripe coupon)"""
        offer_data = {
            "offer_type": "discount",
            "description": f"{discount_value} {'%' if discount_type == 'percentage' else ''} off",
            "discount_type": discount_type,
            "discount_value": int(discount_value) if discount_type == 'percentage' else int(discount_value * 100),
            "duration_type": "forever"
        }

        offer = self.razorpay_client.offer.create(offer_data)
        return offer['id']
    

    def create_subscription(self, customer_id: str, plan_name: str, currency: str, total_amount: float, years: int, phone_number: str, user_email: str, notes: Optional[Dict] = None) -> Dict:

        """Create subscription in Razorpay"""
        try:
            # success_url = f"{WEB_APP_URI}/checkout_success?flag=success&session_id={{CHECKOUT_SESSION_ID}}&user_id={customer_id}"
            # cancel_url = f"{WEB_APP_URI}/checkout_failure?flag=failure&session_id={{CHECKOUT_SESSION_ID}}&user_id={customer_id}"

            amount_in_paise = int(total_amount * 100)
            print(f"[*] Total amount for subscription: {total_amount} {currency.upper()} over {years} years")
            # Create recurring Razorpay subscription plan
            razorpay_plan = self.get_or_create_plan(
                plan_name=plan_name,
                amount=total_amount,
                interval="yearly",
                interval_count=years,
                currency=currency
            )
            # razorpay_plan = self.get_or_create_plan(
            #     plan_name=plan_name,
            #     amount=total_amount,
            #     interval="daily",
            #     interval_count=7,
            #     currency=currency
            # )

            base_plan_id = razorpay_plan

            # Flatten notes
            notes = {k: str(v) for k, v in (notes or {}).items()}

            # Create subscription
            sub_data = {
                "plan_id": base_plan_id,
                "customer_id": customer_id,
                "total_count": 12,
                "quantity": 1,
                "notes": notes,
                "customer_notify": True,
                "notify_info": {
                    "notify_phone": phone_number,
                    "notify_email": user_email
                }
            }

            # sub_data = {
            #     "plan_id": "plan_RtNtqm2RJKHFTx",
            #     "total_count": 100,
            #     "offer_id": "offer_RtNnxyL5R8TB2H",
            #     "upfront_amount": 30000,
            #     "customer_notify": 1
            # }
            subscription = self.razorpay_client.subscription.create(sub_data)
            url = {"url" : WEB_APP_URI + '/checkout_failure?flag=failure&from=employer&session_id={CHECKOUT_SESSION_ID}&user_id=' + user_email,}
            subscription.update(url)    
            return api_json_response_format(
                True,
                "Subscription created",
                200,
                subscription
            )

        except Exception as e:
            print(f"[x] Razorpay subscription creation failed: {str(e)}")
            return api_json_response_format(False, f"{str(e)}", 500, {})
        
class StripeWebhookHandler:
    """
    Handles all Stripe webhook events.
    Separated from payment processing for modularity.
    """

    def __init__(self, promotion_engine: PromotionEngine):
        self.promotion_engine = promotion_engine

    def handle_webhook(self, request):
        """Main webhook handler for Stripe"""
        try:
            signature = request.headers.get('stripe-signature')
            event = stripe.Webhook.construct_event(
                payload=request.data,
                sig_header=signature,
                secret=WH_SEC_KEY
            )

            print(f"[*] Stripe Webhook: {event['type']}")

            if event['type'] == 'checkout.session.completed':
                return self.handle_checkout_completed(event['data']['object'])
            
            elif event['type'] == 'invoice.upcoming':
                return self.handle_invoice_created(event['data']['object'])
            
            elif event['type'] == 'invoice.payment_succeeded':
                return self.handle_invoice_payment_succeeded(event['data']['object'])

            elif event['type'] == 'invoice.payment_failed':
                return self.handle_invoice_payment_failed(event['data']['object'])
            
            
            elif event['type'] == 'customer.subscription.deleted':
                return self.handle_subscription_deleted(event['data']['object'])

            elif event['type'] == 'customer.subscription.trial_will_end':
                return self.handle_subscription_trial_will_end(event['data']['object'])

            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Stripe webhook error: {e}")
            return jsonify({"status": "failed", "error": str(e)}), 500

    

    def handle_subscription_renewal(self, subscription_id: str):
        """
        Handle subscription renewal logic
        -> If initial purchase was 2 years, renewal should be 1 year at base price with 20% discount
        -> If initial purchase was 1 year, renewal should continue at same rate with 20% discount
        """
        try:
            # Get subscription details
            subscription = stripe.Subscription.retrieve(subscription_id)
            
            print(f"[*] Processing renewal for subscription: {subscription_id}")
            
            # Get subscription metadata
            initial_years = int(subscription.metadata.get('initial_years', 1))
            plan_name = subscription.metadata.get('plan_name')
            currency = subscription.metadata.get('currency', 'inr')
            
            print(f"[*] Initial years: {initial_years}, Plan: {plan_name}")
            
            # Only apply renewal discount if initial purchase was 1 years
            if initial_years >= 1:
                print(f"[*] Applying renewal pricing for {plan_name}")
                
                # Get the main plan item (not addons)
                main_item = None
                for item in subscription['items']['data']:
                    if 'assisted' not in item.metadata.get('plan_name', '').lower():
                        main_item = item
                        break
                
                if not main_item:
                    print(f"[!] Could not find main plan item")
                    return jsonify({"status": "failed", "error": "main plan item not found"}), 500
                
                # Get product
                product_id = main_item.price.product
                
                # Get base price from database
                query = "SELECT * FROM payment_plans WHERE plan = %s"
                plan = execute_query(query, (plan_name,))
                if not plan:
                    print(f"[!] Plan '{plan_name}' not found in DB")
                    return jsonify({"status": "failed", "error": "plan not found"}), 500
                
                plan = plan[0]

                base_price_value = float(
                    plan["base_price_inr"] if currency.lower() == "inr"
                    else plan["base_price_usd"]
                ) * initial_years
                
                # Calculate promotions using promotion engine
                promotions = self.promotion_engine.calculate_all_promotions(
                    context=PromotionContext(
                        plan_name=plan_name,
                        transaction_type='renewal',
                        original_amount=base_price_value
                    )
                )
                
                renewal_discount_type = 'percentage'
                renewal_discount_value = 20   if plan_name == 'Premium' else 25
                
                # Check if renewal_discount promotion exists
                if promotions and 'renewal_discount' in promotions and promotions['renewal_discount']:
                    renewal_promo = promotions['renewal_discount']
                    renewal_discount_type = renewal_promo.discount_type
                    renewal_discount_value = float(renewal_promo.discount_value)
                    print(f"[*] Using promotion: {renewal_promo.promotion_id} - {renewal_discount_value}% off")
                else:
                    print(f"[*] No renewal promotion found, using default 20% off")

                from .payment_process_new import StripePaymentProcessor
                payment = StripePaymentProcessor(self.promotion_engine)


                renewal_price_id = payment.get_or_create_price(
                    product_id, plan_name, currency, initial_years
                )
                print(f"[*] Created/Retrieved renewal price: {renewal_price_id}")
                
               
                # renewal_discount_type = 'percentage'
                # renewal_discount_value = 20  

                renewal_coupon_id = payment.create_coupon(
                    coupon_name=f"{plan_name} renewal discount",
                    coupon_type=renewal_discount_type,
                    coupon_value=renewal_discount_value,
                    currency=currency,
                    product_id=product_id  
                )
            
                print(f"[*] Created renewal coupon: {renewal_coupon_id} - {renewal_discount_value}% off")

                time.sleep(15)
               
                stripe.Subscription.modify(
                    subscription_id,
                    items=[{
                        'id': main_item.id,
                        'price': renewal_price_id,
                        'quantity': 1
                    }],
                    discounts=[{"coupon": renewal_coupon_id}], 
                    metadata={
                        'initial_years': str(initial_years),
                        'plan_name': plan_name,
                        'currency': currency,
                        'renewal_applied': 'true',
                        'renewal_discount_type': renewal_discount_type,
                        'renewal_discount_value': str(renewal_discount_value)
                    }
                )
                
                print(f"[*] Updated subscription {subscription_id} with renewal pricing (base price + {renewal_discount_value}% coupon)")
                return jsonify({"status": "success"}), 200
            
            else:
                print(f"[*] No renewal discount needed (initial purchase was {initial_years} year)")
                return jsonify({"status": "success"}), 200
        
        except Exception as e:
            print(f"[x] Error handling subscription renewal: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({"status": "failed", "error": str(e)}), 500

    def handle_invoice_created(self, invoice):
        """
        Handle invoice.created webhook for subscription renewal.
        Stripe sends this BEFORE payment succeeds.
        """
        try:
            subscription_id = invoice.get("subscription")

            if not subscription_id:
                return jsonify({"status": "failed", "error": "subscription not found"}), 500

            billing_reason = invoice.get("billing_reason")
            # Correct and reliable renewal check
            # is_renewal = billing_reason == "subscription_cycle"
            is_renewal = billing_reason == "upcoming"

            if is_renewal:
                print(f"[*] Renewal invoice created for subscription: {subscription_id}")
                return self.handle_subscription_renewal(subscription_id)

            # Not a renewal (initial invoice or something else)
            print(f"[*] Invoice created (not renewal). Billing reason: {billing_reason}")
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error processing invoice.created webhook: {str(e)}")
            return jsonify({"status": 'failed', "error": str(e)}), 500

        
    def handle_checkout_completed(self, data_object: Dict):
        """Handle checkout.session.completed - Initial purchase"""
        try:
            email_id = data_object['customer_details']['email']
            user_data = get_user_data(email_id)

            is_sub_user = False
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
                is_sub_user = True

            user_id = int(user_data['user_id'])
            print(f"[*] Handling checkout completed for user_id: {user_id}, email: {email_id}")
            metadata = data_object.get('metadata', {})
            plan_name = metadata.get("plan")
            old_plan = metadata.get("old_plan")
            action = metadata.get("action")
            total_jobs = int(metadata.get("total_jobs") or 0)
            assisted_job_count = int(metadata.get("assisted_job_count") or 0)
            addon_job_count = int(metadata.get("addon_job_count") or 0)
            validity_year = int(metadata.get("years") or 1)
            gst_amount = float(metadata.get("gst_amount") or 0)
            amount_total = data_object.get('amount_total') or 0
            total_amount = float(amount_total) / 100
            payment_method = metadata.get("payment_method")
            currency = data_object['currency']
            payment_created_date = data_object['created']
            checkout_status = data_object['status']
            # Get subscription ID
            if data_object['mode'] == 'payment':
                subscription_id = data_object.get('payment_intent')
            else:
                subscription_id = data_object.get('subscription')

            if plan_name == 'Trialing':
                print(f"[*] Trial plan selected, no subscription recording needed for user_id: {user_id}")

               
                update_query(
                    "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, subscription_id = %s  WHERE user_id = %s",
                    ('trialing', 'Basic', currency, 'Y', subscription_id, user_id,)
                )
                
                update_query(
                    "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, subscription_id = %s  WHERE user_id = %s",
                    ('trialing', 'Basic', currency, 'Y', subscription_id, user_id,)
                )

                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, 'Trial', total_jobs, assisted_job_count,
                    subscription_id or '', 0, payment_created_date
                )

                return jsonify({"status": "success"}), 200
            recorded_subscription_id = None

            validity_end = payment_created_date + (validity_year * 365 * 24 * 60 * 60)

            checkout_status_msg = ""

            user_plan_details = """
            SELECT 
                u.*,
                upd.*
            FROM users u
            LEFT JOIN user_plan_details upd 
                ON u.user_id = upd.user_id
            WHERE u.user_id = %s;
            """
            user_plan_result = execute_query(user_plan_details, (user_id,))
            if user_plan_result:
                temp_total_jobs = user_plan_result[0]['total_jobs']
                temp_no_of_jobs = user_plan_result[0]['no_of_jobs']
                temp_addional_jobs_count = user_plan_result[0]['additional_jobs_count']
                temp_assisted_jobs_allowed = user_plan_result[0]['assisted_jobs_allowed']
                temp_assisted_jobs_used = user_plan_result[0]['assisted_jobs_used']
                temp_current_period_start = user_plan_result[0]['current_period_start']
                temp_current_period_end = user_plan_result[0]['current_period_end']

                

            if action == 'new_plan_subscription':

                
                cancellation_manager = SubscriptionCancellation()
                cancellation_manager.cancel_subscription(user_id, 'stripe', email_id, action)

                time.sleep(5)

                print(f"[*] Cancelled existing subscription for user_id: {user_id}")
                print(f"[*] Processing new plan subscription for user_id: {user_id}, plan: {plan_name}")
                # Record subscription
                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    subscription_id or '', validity_year, payment_created_date
                )
                print(f"[*] Recorded subscription ID: {recorded_subscription_id}")

                
                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s, subscription_id = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', subscription_id, user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
               
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s, subscription_id = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', subscription_id, user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")

                

                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {temp_plan_name} plan!"
            
            elif action in ['upgrade', 'downgrade']:

                print(f"[*] Processing plan {action} for user_id: {user_id}, new plan: {plan_name}")
                # Cancel existing subscription 
                
                cancellation_manager = SubscriptionCancellation()
                cancellation_manager.cancel_subscription(user_id, 'stripe', email_id, action)
                time.sleep(5)
                print(f"[*] Cancelled existing subscription for user_id: {user_id}")

                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s, subscription_id = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', subscription_id, user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
                
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s, subscription_id = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', subscription_id, user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    subscription_id or '', validity_year, payment_created_date
                )
                print(f"[*] Recorded new subscription ID: {recorded_subscription_id}")

                
                print(f"[*] Transferred subuser jobs to owner for user_id: {user_id}")
                # if not subscription_result:

                    
                #     user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                #     user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                #     update_query(user_query,user_values)
                #     print(f"[*] Updated user payment details for user_id: {user_id}")

                #     user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                #     user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                #     row_count = update_query(user_plan_query,user_plan_values)
                #     print(f"[*] Updated user plan details for user_id: {user_id}")

                #     subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                #     subscription_result = execute_query(subscription_query, (user_id, 'active'))
                
                # if subscription_result:
                #     subscription_data = subscription_result[0]
                #     recorded_subscription_id = subscription_data['id']
                
                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"You have successfully signed up for the {temp_plan_name} plan!"

            elif action == 'addon_jobs':
                print(f"[*] Processing addon jobs for user_id: {user_id}, addon jobs: {addon_job_count}")
                # Add additional jobs to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC LIMIT 1"
                subscription_result = execute_query(subscription_query, (user_id,))

                
                if not subscription_result:
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_addional_jobs_allowed,
                    subscription_id or '', validity_year, temp_current_period_start
                )
                    # if not is_sub_user:
                    #     query = 

                    # raise Exception(f"No active subscription found for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC LIMIT 1"
                subscription_result = execute_query(subscription_query, (user_id,))

                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']

                # Update jobs allowed
                new_total_jobs = subscription_data['jobs_allowed'] + addon_job_count
                update_query(
                    "UPDATE subscriptions SET jobs_allowed = %s, remaining_jobs = remaining_jobs + %s WHERE id = %s",
                    (new_total_jobs, addon_job_count, recorded_subscription_id)
                )

                user_plan_query = "UPDATE user_plan_details SET no_of_jobs = no_of_jobs + %s, additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                user_plan_values = (addon_job_count, addon_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)

                checkout_status_msg = "You have successfully added an additional post(s) to your account!"

            elif action == 'assisted_jobs':
                print(f"[*] Processing assisted jobs for user_id: {user_id}, assisted jobs: {assisted_job_count}")
                # Add assisted job to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC LIMIT 1"
                subscription_result = execute_query(subscription_query, (user_id, ))

                if not subscription_result:
                    # raise Exception(f"No active subscription found for user_id: {user_id}")
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_addional_jobs_allowed,
                    subscription_id or '', validity_year, temp_current_period_start
                )
                    
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC LIMIT 1"
                subscription_result = execute_query(subscription_query, (user_id, ))

                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']

                # Update assisted jobs allowed
                new_assisted_jobs_allowed = subscription_data['assisted_jobs_allowed'] + assisted_job_count
                update_query(
                    "UPDATE subscriptions SET assisted_jobs_allowed = %s WHERE id = %s",
                    (new_assisted_jobs_allowed, recorded_subscription_id)
                )

                user_plan_query = "UPDATE user_plan_details SET assisted_jobs_allowed = assisted_jobs_allowed + %s WHERE user_id = %s"
                user_plan_values = (assisted_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'assisted_jobs', assisted_job_count, created_at)
                update_query(addon_query, addon_values)

                checkout_status_msg = "You have successfully added assisted job(s) to your account!"
            else:
                raise Exception(f"Invalid action type: {action}")

            

           
            if row_count > 0:
                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                created_at = datetime.now()                    
                values = (user_id, checkout_status_msg,created_at,)
                update_query(query,values)

            # Record payment
            payment_method = data_object.get('payment_method_types', ['card'])[0] if data_object.get('payment_method_types') else 'card'
            payment_id = self.record_payment(
                user_id, plan_name, action, recorded_subscription_id, currency,
                gst_amount, total_amount, payment_method, 'stripe', subscription_id
            )

            # Record applied promotions from metadata
            self.record_applied_promotions(user_id, metadata, plan_name, action, payment_id, 'completed')

            # # Record user plan history
            # history_query = """INSERT INTO user_plan_history (user_id, payment_id, subscription_id, old_plan, new_plan, action, old_validity_end, new_validity_start, new_validity_end, old_jobs_remaining, new_jobs_allocated, currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            # old_plan = metadata.get("old_plan", "")
            # old_validity_end = metadata.get("old_validity_end   ", 0)
            # new_validity_start = payment_created_date
            # new_validity_end = metadata.get("new_validity_end", 0)      
            # old_jobs_remaining = metadata.get("old_jobs_remaining", 0)
            # new_jobs_allocated = total_jobs
            # history_values = (user_id, payment_id, recorded_subscription_id, old_plan, plan_name, action, old_validity_end, new_validity_start, new_validity_end, old_jobs_remaining, new_jobs_allocated, currency)
            # update_query(history_query, history_values)

            print(f"[*] Stripe payment processed. Payment ID: {payment_id}")

            if action in ['downgrade']:
               # Transfer subusers jobs to owner
                transfer_jobs_to_owner(user_id, action)

            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling checkout completed: {str(e)}")
            return jsonify({"status": "failed", "error": str(e)}), 500

    def handle_invoice_payment_succeeded(self, data_object: Dict):
        """Handle invoice.payment_succeeded - Renewal"""
        try:
            stripe_subscription_id = data_object['subscription']
            if not stripe_subscription_id:
                return jsonify({"status": "no_subscription"}), 200
            status = 'succeeded'
            status = data_object.get("status")
            # Detect renewal
            billing_reason = data_object.get("billing_reason")
            invoice_number = data_object.get("invoice_number", 1)

            is_renewal = (
                billing_reason == "subscription_cycle"
            )

            if not is_renewal:
                print("[*] Not a renewal invoice. Ignoring.")
                return jsonify({"status": "not_renewal"}), 200

            # Fetch DB subscription
            query = "SELECT * FROM subscriptions WHERE stripe_subscription_id = %s"
            result = execute_query(query, (stripe_subscription_id,))


            if not result:
                print(f"[!] Subscription not found: {stripe_subscription_id}")
                return jsonify({"status": "subscription_not_found"}), 404

            subscription_data = result[0]
            user_id = subscription_data['user_id']

            query = "SELECT * FROM users WHERE user_id = %s"
            user_result = execute_query(query, (user_id,))
            if not user_result:
                query = "SELECT * FROM sub_users WHERE user_id = %s"
                user_result = execute_query(query, (user_id,))
                if not user_result:
                    print(f"[!] No user found for user_id: {user_id}")
                    return jsonify({"status": "user_not_found"}), 404
            user_data = user_result[0]
            print(f"[*] Processing renewal payment for user_id: {user_id}, subscription_id: {stripe_subscription_id}")
            email_id = user_data['email_id']
            # Update subscription validity
            period_end = data_object['period_end']
            update_query(
                "UPDATE subscriptions SET validity_end = %s WHERE id = %s",
                (period_end, subscription_data['id'])
            )

            update_query(
                "UPDATE users SET current_period_end = %s WHERE user_id = %s",
                (period_end, user_id)
            )

            update_query(
                "UPDATE sub_users SET current_period_end = %s WHERE user_id = %s",
                (period_end, user_id)
            )

            # Process payment
            amount = float(data_object['amount_paid'] / 100)
            currency = data_object['currency']

            payment_id = self.record_payment(
                user_id, subscription_data['plan'], 'renewal',
                subscription_data['id'], currency, 0, amount,
                'card', 'stripe', stripe_subscription_id
            )
            
            
            print(f"[*] Renewal payment processed. Payment ID: {payment_id}")

            checkout_status_msg = f"Your subscription has been successfully renewed. Thank you for staying with us!"
            
            print("Payment status : "+str(status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
            query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
            created_at = datetime.now()                    
            values = (user_id,checkout_status_msg,created_at,)
            update_query(query,values)
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling invoice payment: {str(e)}")
            return jsonify({"status": "failed", "error": str(e)}), 500

    def handle_invoice_payment_failed(self, data_object: Dict):
        """Handle invoice.payment_failed"""
        try:
            stripe_subscription_id = data_object['subscription']

            query = "SELECT * FROM users WHERE subscription_id = %s"
            user_result = execute_query(query, (stripe_subscription_id,))
            if not user_result:
                query = "SELECT * FROM sub_users WHERE subscription_id = %s"
                user_result = execute_query(query, (stripe_subscription_id,))
            if not user_result:
                print(f"[!] No user found for subscription_id: {stripe_subscription_id}")
                return jsonify({"status": "user_not_found"}), 404
            
            user_id = user_result[0]['user_id']

            update_query(
                "UPDATE subscriptions SET status = 'payment_failed' WHERE stripe_subscription_id = %s",
                (stripe_subscription_id,)
            )

            update_query(
                "UPDATE users SET payment_status = %s WHERE user_id = %s",
                ('active', user_id)
            )

            update_query(
                "UPDATE sub_users SET payment_status = %s WHERE user_id = %s",
                ('active', user_id)
            )
            print(f"[*] Subscription payment failed: {stripe_subscription_id}")
            return jsonify({"status": "success"}), 200
        except Exception as e:
            print(f"[x] Error handling invoice payment failure: {str(e)}")
            return jsonify({"status": "failed"}), 500

    def handle_subscription_deleted(self, data_object: Dict):
        """Handle customer.subscription.deleted"""
        try:
            stripe_subscription_id = data_object['id']
            metadata = data_object.get("metadata", {})
            cancelled_by = metadata.get("cancelled_by")

            email_id = metadata.get("user_email")
            if not email_id:
                customer_id = data_object.get('customer')
                customer = stripe.Customer.retrieve(customer_id)
                email_id = customer.get('email')
            plan_name = metadata.get("plan")

            if not plan_name:
                plan_name = metadata.get("plan_name")
            

            user_data = get_user_data(email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
            user_id = int(user_data['user_id'])
            created_at = datetime.now()

            if plan_name == 'Trialing':
                print(f"[*] Trial plan cancelled, no subscription update needed for subscription_id: {stripe_subscription_id}")

                update_query(
                "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s,%s,%s)",
                (
                    user_id,
                    "Your trial period will end soon. Please subscribe to continue enjoying all features.",
                    created_at
                )
            )

                update_query("UPDATE users SET payment_status = %s WHERE user_id = %s", ('trial_expired', user_id))
                update_query("UPDATE sub_users SET payment_status = %s WHERE user_id = %s", ('trial_expired', user_id))
            
            
            if cancelled_by != None and cancelled_by not in ['upgrade', 'downgrade', 'new_plan_subscription']:
                update_query(
                "UPDATE subscriptions SET status = 'cancelled' WHERE user_id = %s",
                (user_id,)
                )
                update_query(
                "UPDATE users SET pricing_category = %s, payment_status = %s , is_cancelled = %s WHERE user_id = %s",
                (plan_name, 'active', 'Y', user_id))

                update_query(
                "UPDATE sub_users SET pricing_category = %s, payment_status = %s , is_cancelled = %s WHERE user_id = %s",
                (plan_name, 'active', 'Y', user_id))

                print(f"[*] Subscription cancelled: {stripe_subscription_id}")
            return jsonify({"status": "success"}), 200
        except Exception as e:
            print(f"[x] Error handling subscription deletion: {str(e)}")
            return jsonify({"status": "failed"}), 500


    def handle_subscription_trial_will_end(self, data_object: Dict):
        """Handle Stripe customer.subscription.trial_will_end"""
        try:
            # subscription = data_object.get('data', {}).get('OBJECT', {})

            if not data_object:
                print("[!] No subscription object found")
                return jsonify({"status": "ignored"}), 200

            metadata = data_object.get('metadata', {})

            email_id = metadata.get('user_email')

            canceled_at = data_object.get('canceled_at')
            
            # Fallback: fetch customer email
            if not email_id:
                customer_id = data_object.get('customer')
                customer = stripe.Customer.retrieve(customer_id)
                email_id = customer.get('email')

            if not email_id:
                raise Exception("Email not found for subscription")

            # Fetch user
            user_query = """
                SELECT u.user_id, u.email_id, ur.user_role
                FROM users u
                JOIN user_role ur ON u.user_role_fk = ur.role_id
                WHERE u.email_id = %s
            """
            user_detail = execute_query(user_query, (email_id,))

            if not user_detail:
                print(f"[!] User not found: {email_id}")
                return jsonify({"status": "user_not_found"}), 200

            user_id = user_detail[0]['user_id']
            user_role = user_detail[0]['user_role']
            created_at = datetime.now()

            # Notify user
            update_query(
                "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES (%s,%s,%s)",
                (
                    user_id,
                    "Your trial period will end soon. Please subscribe to continue enjoying all features.",
                    created_at
                )
            )

            # update_query("UPDATE users SET payment_status = %s WHERE user_id = %s", ('trial_expired', user_id))
            
            # Email
            background_runner.send_plan_end_email(email_id)

            print(f"[*] Stripe trial ending notification sent to {email_id}")
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling subscription trial will end: {str(e)}")
            return jsonify({"status": "failed"}), 500

    def record_subscription(self, user_id: int, plan: str, action: str, total_jobs: int,
                            assisted_job_count: int, stripe_subscription_id: str,
                            validity_year: int, created_date: int) -> int:
        """Record subscription in database"""
        validity_end = created_date + (validity_year * 365 * 24 * 60 * 60)

        query = """
            INSERT INTO subscriptions (
                user_id, plan, validity_year, validity_start, validity_end,
                jobs_allowed, remaining_jobs, assisted_jobs_allowed,
                stripe_subscription_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
        """
        result = update_query_last_index(query, (
            user_id, plan, validity_year, created_date, validity_end,
            total_jobs, total_jobs, assisted_job_count, stripe_subscription_id
        ))

        return result['last_index']

    def record_payment(self, user_id: int, plan: str, action: str, subscription_id: int,
                       currency: str, gst_amount: float, total_amount: float,
                       payment_method: str, gateway: str, transaction_id: str) -> int:
        """Record payment in database"""
        query = """
            INSERT INTO payments (
                user_id, plan, payment_type, subscription_id, currency,
                gst_amount, total_amount, gateway, payment_method,
                transaction_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'complete')
        """
        result = update_query_last_index(query, (
            user_id, plan, action, subscription_id, currency,
            float(gst_amount), float(total_amount), gateway, payment_method, transaction_id
        ))

        return result['last_index']

    def record_applied_promotions(self, user_id: int, metadata: Dict, plan: str,
                                  action: str, payment_id: int, payment_status: str) -> None:
        """Record applied promotions from checkout metadata"""
        promotions_json = metadata.get('promotions_applied', '{}')

        if promotions_json and promotions_json != '{}':
            try:
                promotions_applied = json.loads(promotions_json)

                for promo_key, promo_data in promotions_applied.items():
                    query = """
                        INSERT INTO applied_promotions (
                            user_id, promotion_type, promotion_rule_id,
                            discount_amount, transaction_type, payment_id,
                            currency, payment_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    update_query(query, (
                        user_id,
                        promo_data.get('type', 'discount'),
                        promo_data.get('id'),
                        float(promo_data.get('discount', 0)),
                        action,
                        payment_id,
                        metadata.get('currency', 'INR'),
                        payment_status
                    ))
            except Exception as e:
                print(f"[!] Error recording promotions: {e}")




class RazorpayWebhookHandler:
    """
    Handles all Razorpay webhook events.
    Mirror logic of Stripe webhook for consistency.
    """

    def __init__(self, promotion_engine: PromotionEngine):
        self.promotion_engine = promotion_engine
        self.razorpay_client = razorpay_client

    import hmac
    import hashlib

    def verify_signature(self, payload, signature, secret):
        try:
            generated_signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(generated_signature, signature)
        except Exception as e:
            print(f"Error verifying signature: {e}")
            return False



    def handle_webhook(self, request):
        """Main webhook handler for Razorpay"""
        try:
            # Get raw body and signature
            payload = request.data.decode('utf-8')
            received_signature = request.headers.get("X-Razorpay-Signature")
            print("razorpay_webhook() called")
            
            if not received_signature or not self.verify_signature(payload, received_signature, RAZORPAY_WEBHOOK_SECRET):
                return jsonify({"error": "Invalid signature"}), 400

            # Parse JSON after verification
            event_data = json.loads(payload)
            event_type = event_data.get('event')
            payload = event_data.get('payload', {})

            print(f"[*] Razorpay Webhook: {event_type}")

            # Handle events
            if event_type == 'payment.captured':
                return self.handle_payment_captured(payload)

            elif event_type == 'payment.failed':
                return self.handle_payment_failed(payload)

            elif event_type == 'subscription.charged':
                return self.handle_subscription_charged(payload)

            elif event_type == 'subscription.failed':
                return self.handle_subscription_failed(payload)
            elif event_type == 'subscription.cancelled':
                return self.handle_subscription_cancelled(payload)

            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Razorpay webhook error: {e}")
            return jsonify({"status": "failed", "error": str(e)}), 500


    def handle_payment_captured(self, payload: Dict):
        """Handle payment.captured - One-time payment or initial subscription"""
        try:
            payment_entity = payload.get('payment', {}).get('entity', {})
            
            notes = payment_entity.get('notes', {})
            if not notes:
                print("[!] No notes found in payment entity")
                return jsonify({"status": "no_notes"}), 200
            
            email_id = notes.get('user_email')
            if not email_id:
                print("[!] No user_email found in payment notes")
                return jsonify({"status": "no_email"}), 200
            user_data = get_user_data(email_id)
            
            is_sub_user = False
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
                is_sub_user = True

            user_id = int(user_data['user_id'])
            user_role = user_data['user_role']
            print(f"User data: {user_data}")
            user_id = int(user_data['user_id'])
            plan_name = notes.get('plan')
            old_plan = notes.get('old_plan')
            action = notes.get('action', 'signup')
            total_jobs = int(notes.get('total_jobs') or 0)
            assisted_job_count = int(notes.get('assisted_job_count') or 0)
            validity_year = int(notes.get('years') or 1)
            addon_job_count = int(notes.get('addon_job_count') or 0)
            checkout_status = 'completed' if payment_entity.get('status') == 'captured' else 'failed'
            # Amount details (convert from paise)
            base_amount = float(notes.get('base_amount') or 0)
            gst_amount = float(notes.get('gst_amount') or 0)
            total_amount = float(payment_entity.get('amount', 0)) / 100
            currency = payment_entity.get('currency', 'INR').lower()
            transaction_id = payment_entity.get('id')
            payment_created_date = payment_entity.get('created_at', int(time.time()))

            # Calculate validity_end
            validity_end = payment_created_date + (validity_year * 365 * 24 * 60 * 60)

            recorded_subscription_id = None

            checkout_status_msg = ""
            
            user_plan_details = """
                SELECT 
                    u.*,
                    upd.*
                FROM users u
                LEFT JOIN user_plan_details upd 
                    ON u.user_id = upd.user_id
                WHERE u.user_id = %s;
            """
            user_plan_result = execute_query(user_plan_details, (user_id,))
            if user_plan_result:
                temp_total_jobs = user_plan_result[0]['total_jobs']
                temp_no_of_jobs = user_plan_result[0]['no_of_jobs']
                temp_addional_jobs_count = user_plan_result[0]['additional_jobs_count']
                temp_assisted_jobs_allowed = user_plan_result[0]['assisted_jobs_allowed']
                
                temp_assisted_jobs_used = user_plan_result[0]['assisted_jobs_used']
                temp_current_period_start = user_plan_result[0]['current_period_start']
                temp_current_period_end = user_plan_result[0]['current_period_end']

            if action == 'new_plan_subscription':

                
                cancellation_manager = SubscriptionCancellation()
                cancellation_manager.cancel_subscription(user_id, 'razorpay', email_id, action)
                time.sleep(5)
                print(f"[*] Processing new plan subscription for user_id: {user_id}, plan: {plan_name}")
                # Record subscription
                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    transaction_id, validity_year, payment_created_date, validity_end
                )
                print(f"[*] Recorded subscription ID: {recorded_subscription_id}")

                
                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
                
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                # razorpay_query = "UPDATE razorpay_customers SET pricing_category = %s, payment_status = %s, subscription_status = %s,  subscription_id = %s, current_period_start = %s, current_period_end = %s, is_trial_started = %s, is_cancelled = %s WHERE email_id = %s"
                razorpay_query = "INSERT INTO razorpay_customers (pricing_category, payment_status, subscription_status, subscription_id, current_period_start, current_period_end, is_trial_started, is_cancelled, email_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pricing_category = VALUES(pricing_category), payment_status = VALUES(payment_status), subscription_status = VALUES(subscription_status), subscription_id = VALUES(subscription_id), current_period_start = VALUES(current_period_start), current_period_end = VALUES(current_period_end), is_trial_started = VALUES(is_trial_started), is_cancelled = VALUES(is_cancelled)"
                razorpay_values = (plan_name, 'active', 'subscription.activated', transaction_id, payment_created_date, validity_end, 'Y', 'N', email_id,)
                update_query(razorpay_query,razorpay_values)
                print(f"[*] Inserted razorpay subscription details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {temp_plan_name} plan!"
            elif action in ['upgrade', 'downgrade']:
                print(f"[*] Processing plan {action} for user_id: {user_id}, new plan: {plan_name}")
                # Cancel existing subscription 
                
                cancellation_manager = SubscriptionCancellation()
                cancellation_manager.cancel_subscription(user_id, 'razorpay', email_id, action)
                time.sleep(5)
                

                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
                  
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                razorpay_query = "INSERT INTO razorpay_customers (pricing_category, payment_status, subscription_status, subscription_id, current_period_start, current_period_end, is_trial_started, is_cancelled, email_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pricing_category = VALUES(pricing_category), payment_status = VALUES(payment_status), subscription_status = VALUES(subscription_status), subscription_id = VALUES(subscription_id), current_period_start = VALUES(current_period_start), current_period_end = VALUES(current_period_end), is_trial_started = VALUES(is_trial_started), is_cancelled = VALUES(is_cancelled)"
                razorpay_values = (plan_name, 'active', 'subscription.activated', transaction_id, payment_created_date, validity_end, 'Y', 'N', email_id,)
                update_query(razorpay_query,razorpay_values)
                print(f"[*] Inserted razorpay subscription details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    transaction_id or '', validity_year, payment_created_date, validity_end)
                print(f"[*] Recorded new subscription ID: {recorded_subscription_id}")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                # if not subscription_result:
                #     print(f"[*] No active subscription found after cancellation for user_id: {user_id}")
                    
                #     user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s WHERE user_id = %s"
                #     user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, user_id,)
                #     update_query(user_query,user_values)
                #     print(f"[*] Updated user payment details for user_id: {user_id}")

                #     user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                #     user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                #     row_count = update_query(user_plan_query,user_plan_values)
                #     print(f"[*] Updated user plan details for user_id: {user_id}")

                #     subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                #     subscription_result = execute_query(subscription_query, (user_id, 'active'))
                
                #     if subscription_result:
                #         subscription_data = subscription_result[0]
                #         recorded_subscription_id = subscription_data['id']
                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"You have successfully signed up for the {temp_plan_name} plan!"

            elif action == 'addon_jobs':
                print(f"[*] Processing addon jobs for user_id: {user_id}, addon jobs: {addon_job_count}")
                # Add additional jobs to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s ORDER BY created_at DESC LIMIT 1"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                if not subscription_result:
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_assisted_jobs_allowed,
                    transaction_id or '', validity_year, temp_current_period_start, validity_end
                )
                    # print(f"[*] No active subscription found for user_id: {user_id}")
                    
                    # raise Exception(f"No active subscription found for user_id: {user_id}")
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']
                print(f"[*] Found active subscription ID: {recorded_subscription_id} for user_id: {user_id}")
                # Update jobs allowed
                new_total_jobs = subscription_data['jobs_allowed'] + addon_job_count
                update_query(
                    "UPDATE subscriptions SET jobs_allowed = %s, remaining_jobs = remaining_jobs + %s WHERE id = %s",
                    (new_total_jobs, addon_job_count, recorded_subscription_id)
                )
                print(f"[*] Updated jobs allowed to {new_total_jobs} for subscription ID: {recorded_subscription_id}")
                user_plan_query = "UPDATE user_plan_details SET no_of_jobs = no_of_jobs + %s, additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                user_plan_values = (addon_job_count, addon_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id} with addon jobs")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                checkout_status_msg = "You have successfully added an additional post(s) to your account!"
            elif action == 'assisted_jobs':
                print(f"[*] Processing assisted jobs for user_id: {user_id}, assisted jobs: {assisted_job_count}")
                # Add assisted job to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))
                print(f"[*] Retrieved subscription result for user_id: {user_id}")

                if not subscription_result:
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_assisted_jobs_allowed,
                    transaction_id or '', validity_year, temp_current_period_start, validity_end
                )
                    # raise Exception(f"No active subscription found for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']
                print(f"[*] Found active subscription ID: {recorded_subscription_id} for user_id: {user_id}")
                # Update assisted jobs allowed
                new_assisted_jobs_allowed = subscription_data['assisted_jobs_allowed'] + assisted_job_count
                update_query(
                    "UPDATE subscriptions SET assisted_jobs_allowed = %s WHERE id = %s",
                    (new_assisted_jobs_allowed, recorded_subscription_id)
                )
                print(f"[*] Updated assisted jobs allowed to {new_assisted_jobs_allowed} for subscription ID: {recorded_subscription_id}")

                user_plan_query = "UPDATE user_plan_details SET assisted_jobs_allowed = assisted_jobs_allowed + %s WHERE user_id = %s"
                user_plan_values = (assisted_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id} with assisted jobs")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'assisted_jobs', assisted_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded assisted jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                checkout_status_msg = "You have successfully added assisted job(s) to your account!"
            else:
                raise Exception(f"Invalid action type: {action}")



            if row_count > 0:
                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                created_at = datetime.now()                    
                values = (user_id,checkout_status_msg,created_at,)
                update_query(query,values)

            payment_method = payment_entity.get('method', 'unknown')
            # Record payment    
            payment_id = self.record_payment(
                user_id, plan_name, action, recorded_subscription_id, currency,
                gst_amount, total_amount, payment_method, 'razorpay', transaction_id
            )
            print(f"[*] Recorded payment ID: {payment_id} for user_id: {user_id}")

            if action in ['downgrade']:
               # Transfer subusers jobs to owner
                transfer_jobs_to_owner(user_id, action)

            # Record applied promotions from metadata
            # self.record_applied_promotions(user_id, notes, plan_name, action, payment_id, currency, 'completed')
            self.record_applied_promotions(user_id, notes, plan_name, action, payment_id, currency, checkout_status)


         
            print(f"[*] Razorpay payment captured. Payment ID: {payment_id}")
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling payment captured: {str(e)}")
            return jsonify({"status": "failed", "error": str(e)}), 500

    def handle_payment_failed(self, payload: Dict):
        """Handle payment.failed"""
        try:
            payment_entity = payload.get('payment', {}).get('entity', {})
            notes = payment_entity.get('notes', {})
            email_id = payment_entity.get('email', '')

            print(f"[!] Razorpay payment failed: {payment_entity.get('id')}")
            print(f"[!] Reason: {payment_entity.get('error_description', 'Unknown')}")

            # email_id = notes.get('user_email', '')
            if email_id:
                user_data = get_user_data(email_id)
                if not user_data['is_exist']:
                    user_data = get_sub_user_data(email_id)
                if user_data:
                    self.log_failed_payment(int(user_data['user_id']), payment_entity)

            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling payment failed: {str(e)}")
            return jsonify({"status": "failed"}), 500
        
    def handle_subscription_renewal(self, subscription_entity: Dict):
        """Handle updates on subscription renewal"""
        try:
            notes = subscription_entity.get('notes', {})
            email_id = notes.get('user_email') or subscription_entity.get('email', '')
            user_data = get_user_data(email_id)

            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)

            if not user_data:
                print(f"[!] User not found: {email_id}")
                return jsonify({"status": "user_not_found"}), 404

            user_id = int(user_data['user_id'])
            plan_name = notes.get('plan')
            validity_year = int(notes.get('years', 1))
            currency = subscription_entity.get('currency', 'INR').lower()
            razorpay_subscription_id = subscription_entity.get('id')
            payment_created_date = subscription_entity.get('created_at', int(time.time()))

            query = "SELECT * FROM payment_plans WHERE plan = %s"
            plan = execute_query(query, (plan_name,))
            if not plan:
                print(f"[!] Plan '{plan_name}' not found in DB")
                return jsonify({"status": "failed", "error": "plan not found"}), 500
            
            plan = plan[0]

            base_price_value = float(
                plan["base_price_inr"] if currency.lower() == "inr"
                else plan["base_price_usd"]
            ) * validity_year

            # Get user_id from subscription
            # query_user = "SELECT id, plan, user_id FROM subscriptions WHERE razorpay_subscription_id = %s"
            # sub_result = execute_query(query_user, (razorpay_subscription_id,))
            # user_id = sub_result[0]['user_id'] if sub_result else None
            # subscription_db_id = sub_result[0]['id'] if sub_result else None
            # subscription_plan = sub_result[0]['plan'] if sub_result else None
            subscription_plan = plan_name
            
            user_data = get_user_data(email_id)
            user_role = user_data['user_role']
            
            # Calculate promotions using promotion engine
            promotions = self.promotion_engine.calculate_all_promotions(
                context=PromotionContext(
                    plan_name=plan_name,
                    subscription_plan=subscription_plan,
                    transaction_type='renewal',
                    original_amount=base_price_value
                )
            )
            
            renewal_discount_type = 'percentage'
            renewal_discount_value = 20  
            
            # Check if renewal_discount promotion exists
            if promotions and 'renewal_discount' in promotions and promotions['renewal_discount']:
                renewal_promo = promotions['renewal_discount']
                renewal_discount_type = renewal_promo.discount_type
                renewal_discount_value = int(renewal_promo.discount_value)
                print(f"[*] Using promotion: {renewal_discount_value}% off")
            else:
                print(f"[*] No renewal promotion found, using default 20% off")

            # query = "SELECT key_value FROM payment_plan_keys WHERE key_name = %s AND plan = %s AND gateway = %s"
            # renewal = execute_query(query, (f"{plan_name.lower()}_renewal_discount_{renewal_discount_value}", plan_name, 'razorpay'))
            # if renewal:
            #     renewal = renewal[0]
            #     renewal_coupon_id = renewal['key_value']
            #     print(f"[*] Found renewal coupon ID: {renewal_coupon_id}")
            # else:
            #     print(f"[!] Renewal coupon not found for {plan_name} with {renewal_discount_value}% discount")
            #     renewal_coupon_id = None
            
            query = "SELECT * FROM payment_plans WHERE plan = %s"
            plan = execute_query(query, (plan_name,))
            if not plan:
                print(f"[!] Plan '{plan_name}' not found in DB")
                return jsonify({"status": "failed", "error": "plan not found"}), 500
            
            plan = plan[0]
            base_price_value = float(
                plan["base_price_inr"] if currency.lower() == "inr"
                else plan["base_price_usd"]
            ) * validity_year

            if renewal_discount_type == 'percentage':
                discount_amount = (base_price_value * renewal_discount_value) / 100
            else:
                discount_amount = renewal_discount_value
            discounted_price = max(base_price_value - discount_amount, 0)
            print(f"[*] Calculated discounted price: {discounted_price} {currency.upper()} (Original: {base_price_value}, Discount: {discount_amount})")
            
            from .payment_process_new import RazorpayPaymentProcessor
            razorpay_processor = RazorpayPaymentProcessor(PromotionEngine())

            razorpay_plan_id = razorpay_processor.get_or_create_plan(
                    plan_name=plan_name,
                    amount=discounted_price,
                    interval="yearly",
                    interval_count=validity_year,
                    currency=currency
                )

            sub_data = {
                "plan_id": razorpay_plan_id,
                "quantity": 12,
                "schedule_change_at": "cycle_end"
            }

            # if renewal_coupon_id:
            #     sub_data["offer_id"] = renewal_coupon_id

            # razorpay_client.subscription.update(razorpay_subscription_id, sub_data)

            import requests
            from requests.auth import HTTPBasicAuth

            url = f"https://api.razorpay.com/v1/subscriptions/{razorpay_subscription_id}"

            payload = {
                "plan_id": razorpay_plan_id,
                "quantity": 12,
                "schedule_change_at": "cycle_end",
                "customer_notify": True
            }

            response = requests.patch(
                url,
                json=payload,
                auth=HTTPBasicAuth(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
                headers={"Content-Type": "application/json"}
            )

            print(response.status_code)
            print(response.json())

            print(f"[*] Razorpay subscription renewal updated for user_id: {user_id}, subscription_id: {razorpay_subscription_id}")
            return jsonify({"status": "success"}), 200
        except Exception as e:
            print(f"[x] Error handling subscription renewal: {str(e)}")
            return jsonify({"status": "failed", "error": str(e)}), 500
        
    def store_refund_request(self, user_id: int, plan_name: str, razorpay_subscription_id: str, refundable_amount: float, currency: str) -> bool:
        """Store refund request in DB"""
        try:
            query = """
                INSERT INTO refund_requests (
                    user_id, plan_name, subscription_id,
                    refundable_amount, currency, status, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
                user_id = VALUES(user_id),
                plan_name = VALUES(plan_name),
                subscription_id = VALUES(subscription_id),
                refundable_amount = VALUES(refundable_amount),
                currency = VALUES(currency),
                status = VALUES(status),
                created_at = VALUES(created_at)
            """

            created_at = datetime.now()
            values = (
                user_id,
                plan_name,
                razorpay_subscription_id,
                refundable_amount,
                currency,
                'pending',
                created_at
            )
            update_query(query, values)
            print(f"[*] Stored refund request for user_id: {user_id}, amount: {refundable_amount} {currency.upper()}")
            return True
        except Exception as e:
            print(f"[x] Error storing refund request: {str(e)}")
            return False
        
    def process_refund(self, user_id: int, plan_name: str, total_amount: float, currency: str, validity_year: int, razorpay_subscription_id: str):
        """Process refund for a subscription"""
        try:
            print(f"[*] Processing refund for user_id: {user_id}, subscription_id: {razorpay_subscription_id}")

            query = "SELECT * FROM payment_plans WHERE plan = %s"
            plan = execute_query(query, (plan_name,))
            if not plan:
                print(f"[!] Plan '{plan_name}' not found in DB")
                return False
            
            plan = plan[0]
            base_price_value = float(
                plan["base_price_inr"] if currency.lower() == "inr"
                else plan["base_price_usd"]
            ) * validity_year

            from .payment_process_new import PromotionEngine, PromotionContext
            promotion_engine = PromotionEngine()
            # Calculate promotions using promotion engine
            promotions = promotion_engine.calculate_all_promotions(
                context=PromotionContext(
                    plan_name=plan_name,
                    transaction_type='renewal'
                )
            )
            evaluate_renewal_discount = 0
            # Check if renewal_discount promotion exists
            if promotions and 'renewal_discount' in promotions and promotions['renewal_discount']:
                renewal_promo = promotions['renewal_discount']
                renewal_discount_type = renewal_promo.discount_type
                renewal_discount_value = int(renewal_promo.discount_value)
                print(f"[*] Using promotion: {renewal_discount_value}% off for refund calculation")

                if renewal_discount_type == 'percentage':
                    evaluate_renewal_discount = (base_price_value * renewal_discount_value) / 100
                else:
                    evaluate_renewal_discount = renewal_discount_value
            else:
                print(f"[*] No renewal promotion found for refund calculation")


            if validity_year == 1:
                base_amount = max(base_price_value - evaluate_renewal_discount, 0)
            elif validity_year >= 2:
                evaluate_renewal_discount = base_price_value * 25 / 100
                base_amount = max(base_price_value - evaluate_renewal_discount, 0)
            else:
                base_amount = 0

            # refundable_amount = total_amount - base_amount
            # refundable_amount = max(refundable_amount, 0)

            gst_amount = (base_amount * 18) / 100
            refundable_amount = max(total_amount - (base_amount + gst_amount), 0)
            print(f"[*] Calculated refundable amount: {refundable_amount} {currency.upper()}")

            if refundable_amount > 0:
                print(f"[*] Refundable amount is greater than zero, proceeding to store refund request")
                store_refund = self.store_refund_request(
                    user_id, plan_name, razorpay_subscription_id, refundable_amount, currency
                    )

                if store_refund:
                    print(f"[*] Refund request stored successfully for user_id: {user_id}")
                else:
                    print(f"[!] Failed to store refund request for user_id: {user_id}")
            else:
                print(f"[*] No refundable amount for user_id: {user_id}, skipping refund request storage")

        except Exception as e:
            print(f"[x] Error processing refund: {str(e)}")
        
    def handle_subscription_charged(self, payload: Dict):
        """Handle subscription.charged - Initial and Renewal"""
        try:
           

            subscription_entity = payload.get('subscription', {}).get('entity', {})
            payment_entity = payload.get('payment', {}).get('entity', {})
            notes = subscription_entity.get('notes', {})

            email_id = notes.get('user_email') or subscription_entity.get('email', '')
            user_data = get_user_data(email_id)

            is_sub_user = False
            if not user_data['is_exist']:
                user_data = get_sub_user_data(email_id)
                is_sub_user = True

            if not user_data['is_exist']:
                print(f"[!] User not found: {email_id}")
                return jsonify({"status": "user_not_found"}), 404

            user_id = int(user_data['user_id'])
            plan_name = notes.get('plan')
            action = notes.get('action', 'signup')
            total_jobs = int(notes.get('total_jobs', 0))
            assisted_job_count = int(notes.get('assisted_job_count', 0))
            validity_year = int(notes.get('years', 1))
            addon_job_count = int(notes.get('addon_job_count', 0))
            old_plan = notes.get('old_plan')
            checkout_status = 'completed' if subscription_entity.get('status') == 'active' else 'failed'
            # Amount details (convert from paise)
            base_amount = float(notes.get('base_amount', '0'))
            gst_amount = float(notes.get('gst_amount', '0'))
            total_amount = float(subscription_entity.get('amount', 0)) / 100
            currency = subscription_entity.get('currency', 'INR').lower()
            razorpay_subscription_id = subscription_entity.get('id')
            payment_created_date = subscription_entity.get('created_at', int(time.time()))

            
            # Calculate validity_end
            validity_end = payment_created_date + (validity_year * 365 * 24 * 60 * 60)

            recorded_subscription_id = None

            checkout_status_msg = ""
            
             # Find first recurring charge or not
            paid_count = subscription_entity.get('paid_count', 0)
            remaining_count = subscription_entity.get('remaining_count', 0)
            total_count = subscription_entity.get('total_count', 0)
            total_amount = payment_entity.get('amount', 0) / 100
            is_first_recurring_charge = (paid_count == 1)

            if remaining_count == (total_count - 1) and (paid_count == 1):
                print(f"[*] This is the first recurring charge for user_id: {user_id}")
                is_first_recurring_charge = True


            if action == 'new_plan_subscription':
                print(f"[*] Processing new plan subscription for user_id: {user_id}, plan: {plan_name}")

                # Record subscription
                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    razorpay_subscription_id, validity_year, payment_created_date, validity_end
                )
                print(f"[*] Recorded subscription ID: {recorded_subscription_id}")
                
               
                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
                
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")
                
                razorpay_query = "INSERT INTO razorpay_customers (pricing_category, payment_status, subscription_status, subscription_id, current_period_start, current_period_end, is_trial_started, is_cancelled, email_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pricing_category = VALUES(pricing_category), payment_status = VALUES(payment_status), subscription_status = VALUES(subscription_status), subscription_id = VALUES(subscription_id), current_period_start = VALUES(current_period_start), current_period_end = VALUES(current_period_end), is_trial_started = VALUES(is_trial_started), is_cancelled = VALUES(is_cancelled)"
                razorpay_values = (plan_name, 'active', 'subscription.activated', razorpay_subscription_id, payment_created_date, validity_end, 'Y', 'N', email_id,)
                update_query(razorpay_query,razorpay_values)
                print(f"[*] Inserted razorpay subscription details for user_id: {user_id}")
                
                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"Thank you for choosing our job portal! We’re excited to support your hiring journey. You’ve successfully signed up for the {temp_plan_name} plan!"
            elif action in ['upgrade', 'downgrade']:

                
                print(f"[*] Processing plan {action} for user_id: {user_id}, new plan: {plan_name}")
                if is_first_recurring_charge:
                    # Cancel existing subscription 
                    cancellation_manager = SubscriptionCancellation()
                    cancellation_manager.cancel_subscription(user_id, 'razorpay', email_id, action)

                time.sleep(5)  # Wait for cancellation to process

                
                user_query = "UPDATE sub_users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated subuser payment details for user_id: {user_id}")
                
                user_query = "UPDATE users SET payment_status = %s, pricing_category = %s, payment_currency = %s, is_trial_started = %s, current_period_start = %s, current_period_end = %s, is_cancelled = %s WHERE user_id = %s"
                user_values = ('active', plan_name, currency, 'Y', payment_created_date, validity_end, 'N', user_id,)
                update_query(user_query,user_values)
                print(f"[*] Updated user payment details for user_id: {user_id}")

                user_plan_query = "UPDATE user_plan_details SET user_plan = %s, no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s, assisted_jobs_allowed = %s, assisted_jobs_used = %s WHERE user_id = %s"
                user_plan_values = (plan_name, total_jobs, total_jobs, addon_job_count, assisted_job_count, 0, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, total_jobs, assisted_job_count,
                    razorpay_subscription_id or '', validity_year, payment_created_date, validity_end)
                print(f"[*] Recorded new subscription ID: {recorded_subscription_id}")

                razorpay_query = "INSERT INTO razorpay_customers (pricing_category, payment_status, subscription_status, subscription_id, current_period_start, current_period_end, is_trial_started, is_cancelled, email_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE pricing_category = VALUES(pricing_category), payment_status = VALUES(payment_status), subscription_status = VALUES(subscription_status), subscription_id = VALUES(subscription_id), current_period_start = VALUES(current_period_start), current_period_end = VALUES(current_period_end), is_trial_started = VALUES(is_trial_started), is_cancelled = VALUES(is_cancelled)"
                razorpay_values = (plan_name, 'active', 'subscription.activated', razorpay_subscription_id, payment_created_date, validity_end, 'Y', 'N', email_id,)
                update_query(razorpay_query,razorpay_values)
                print(f"[*] Inserted razorpay subscription details for user_id: {user_id}")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                temp_plan_name = 'Express' if plan_name == 'Basic' else plan_name
                checkout_status_msg = f"You have successfully signed up for the {temp_plan_name} plan!"
            elif action == 'addon_jobs':
                print(f"[*] Processing addon jobs for user_id: {user_id}, addon jobs: {addon_job_count}")
                # Add additional jobs to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                if not subscription_result:
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_assisted_jobs_allowed,
                    transaction_id or '', validity_year, temp_current_period_start, validity_end
                )
                    # raise Exception(f"No active subscription found for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))

                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']
                print(f"[*] Found active subscription ID: {recorded_subscription_id} for user_id: {user_id}")
                # Update jobs allowed
                new_total_jobs = subscription_data['jobs_allowed'] + addon_job_count
                update_query(
                    "UPDATE subscriptions SET jobs_allowed = %s, remaining_jobs = remaining_jobs + %s WHERE id = %s",
                    (new_total_jobs, addon_job_count, recorded_subscription_id)
                )
                print(f"[*] Updated jobs allowed to {new_total_jobs} for subscription ID: {recorded_subscription_id}")
                user_plan_query = "UPDATE user_plan_details SET no_of_jobs = no_of_jobs + %s, additional_jobs_count = additional_jobs_count + %s WHERE user_id = %s"
                user_plan_values = (addon_job_count, addon_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id} with addon jobs")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'additional_jobs', addon_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded addon jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                checkout_status_msg = "You have successfully added an additional post(s) to your account!"
            elif action == 'assisted_jobs':
                print(f"[*] Processing assisted jobs for user_id: {user_id}, assisted jobs: {assisted_job_count}")
                # Add assisted job to existing subscription
                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))
                print(f"[*] Retrieved subscription result for user_id: {user_id}")

                if not subscription_result:
                    recorded_subscription_id = self.record_subscription(
                    user_id, plan_name, action, temp_total_jobs, temp_assisted_jobs_allowed,
                    transaction_id or '', validity_year, temp_current_period_start, validity_end
                )
                    # raise Exception(f"No active subscription found for user_id: {user_id}")

                subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = %s"
                subscription_result = execute_query(subscription_query, (user_id, 'active'))
                
                subscription_data = subscription_result[0]
                recorded_subscription_id = subscription_data['id']
                print(f"[*] Found active subscription ID: {recorded_subscription_id} for user_id: {user_id}")
                # Update assisted jobs allowed
                new_assisted_jobs_allowed = subscription_data['assisted_jobs_allowed'] + assisted_job_count
                update_query(
                    "UPDATE subscriptions SET assisted_jobs_allowed = %s WHERE id = %s",
                    (new_assisted_jobs_allowed, recorded_subscription_id)
                )
                print(f"[*] Updated assisted jobs allowed to {new_assisted_jobs_allowed} for subscription ID: {recorded_subscription_id}")

                user_plan_query = "UPDATE user_plan_details SET assisted_jobs_allowed = assisted_jobs_allowed + %s WHERE user_id = %s"
                user_plan_values = (assisted_job_count, user_id)
                row_count = update_query(user_plan_query,user_plan_values)
                print(f"[*] Updated user plan details for user_id: {user_id} with assisted jobs")

                addon_query = "INSERT INTO subscription_addons (user_id, subscription_id, addon_type, addon_count, created_at) VALUES (%s, %s, %s, %s, %s)"
                created_at = datetime.now()
                addon_values = (user_id, recorded_subscription_id, 'assisted_jobs', assisted_job_count, created_at)
                update_query(addon_query, addon_values)
                print(f"[*] Recorded assisted jobs for user_id: {user_id}, subscription ID: {recorded_subscription_id}")

                checkout_status_msg = "You have successfully added assisted job(s) to your account!"
            else:
                raise Exception(f"Invalid action type: {action}")



            if row_count > 0:
                print("Payment status : "+str(checkout_status)+", for the user : "+str(email_id)+", Message : "+str(checkout_status_msg)) 
                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                created_at = datetime.now()                    
                values = (user_id,checkout_status_msg,created_at,)
                update_query(query,values)

            payment_method = subscription_entity.get('method', 'unknown')
            # Record payment    
            payment_id = self.record_payment(
                user_id, plan_name, action, recorded_subscription_id, currency,
                gst_amount, total_amount, payment_method, 'razorpay', razorpay_subscription_id
            )
            print(f"[*] Recorded payment ID: {payment_id} for user_id: {user_id}")

            if action in ['downgrade']:
               # Transfer subusers jobs to owner
                transfer_jobs_to_owner(user_id, action)

            # Record applied promotions from metadata
            # self.record_applied_promotions(user_id, notes, plan_name, action, payment_id, currency, 'completed')
            self.record_applied_promotions(user_id, notes, plan_name, action, payment_id, currency, checkout_status)


            # self.handle_subscription_renewal(subscription_entity)


            if not is_first_recurring_charge:
                # print(f"[*] Processing refund for user_id: {user_id}, subscription_id: {razorpay_subscription_id}")
                self.process_refund(user_id, plan_name, total_amount, currency, validity_year, razorpay_subscription_id)

            print(f"[*] Razorpay payment captured. Payment ID: {payment_id}")
            return jsonify({"status": "success"}), 200
        except Exception as e:
            print(f"[x] Error handling subscription charged: {str(e)}")
            return jsonify({"status": "failed"}), 500
        
    def handle_subscription_failed(self, payload: Dict):
        """Handle subscription.failed"""
        try:
            subscription_entity = payload.get('subscription', {}).get('entity', {})
            razorpay_subscription_id = subscription_entity.get('id')

            update_query(
                "UPDATE subscriptions SET status = 'past_due' WHERE razorpay_subscription_id = %s",
                (razorpay_subscription_id,)
            )

            print(f"[*] Razorpay subscription payment failed: {razorpay_subscription_id}")
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling subscription failed: {str(e)}")
            return jsonify({"status": "failed"}), 500
        
    def handle_subscription_cancelled(self, payload: Dict):
        """Handle subscription.cancelled"""
        try:
            subscription_entity = payload.get('subscription', {}).get('entity', {})
            razorpay_subscription_id = subscription_entity.get('id')
            customer_id = subscription_entity.get('customer_id')
            query = "SELECT * FROM subscriptions WHERE razorpay_subscription_id = %s"
            result = execute_query(query, (razorpay_subscription_id,))
            if not result:
                query = "SELECT email_id FROM razorpay_customers WHERE customer_id = %s"
                result = execute_query(query, (customer_id,))
                if not result:
                    print(f"[!] Subscription not found: {razorpay_subscription_id}")
                    return jsonify({"status": "subscription_not_found"}), 404
                email_id = result[0]['email_id']
                user_data = get_user_data(email_id)
                if not user_data:
                    print(f"[!] User not found for email: {email_id}")
                    return jsonify({"status": "user_not_found"}), 404
                user_id = int(user_data['user_id'])
                plan_name = user_data['pricing_category']

                

            else:
                subscription_data = result[0]
                user_id = subscription_data['user_id']
                plan_name = subscription_data['plan']
            

            print(f"[*] Processing subscription cancellation for user_id: {user_id}, subscription_id: {razorpay_subscription_id} from webhook")

            update_query(
                "UPDATE subscriptions SET status = 'cancelled' WHERE razorpay_subscription_id = %s",
                (razorpay_subscription_id,)
            )

            update_query(
                "UPDATE users SET pricing_category = %s, payment_status = %s , is_cancelled = %s WHERE user_id = %s",
                (plan_name, 'active', 'Y', user_id)
            )

            update_query(
                "UPDATE sub_users SET pricing_category = %s, payment_status = %s, is_cancelled = %s WHERE user_id = %s",
                (plan_name, 'active', 'Y', user_id)
            )

            updated_at = datetime.now()
            update_status_query = "UPDATE razorpay_customers SET subscription_status = %s, is_cancelled = %s, updated_at = %s WHERE subscription_id = %s"
            update_values = ('subscription.cancelled', 'Y', updated_at , razorpay_subscription_id)
            update_query(update_status_query, update_values)

            update_sub_users_table = "UPDATE sub_users SET is_cancelled = %s WHERE user_id = %s"
            update_sub_users_values = ('Y', user_id)
            update_query(update_sub_users_table, update_sub_users_values)
            
            print(f"[*] Razorpay subscription cancelled: {razorpay_subscription_id}")
            return jsonify({"status": "success"}), 200

        except Exception as e:
            print(f"[x] Error handling subscription cancelled: {str(e)}")
            return jsonify({"status": "failed"}), 500

    # def payment_exists(transaction_id: str) -> bool:
    #     q = "SELECT id FROM payments WHERE transaction_id = %s AND gateway = 'razorpay'"
    #     return bool(execute_query(q, (transaction_id,)))

    # def handle_payment_captured(self, payload: Dict):
    #     try:
    #         payment = payload['payment']['entity']
    #         transaction_id = payment['id']

    #         #  Idempotency
    #         if payment_exists(transaction_id):
    #             return jsonify({"status": "duplicate"}), 200

    #         notes = payment.get('notes', {})
    #         action = notes.get('action')

    #         # ❌ DO NOT handle subscriptions here
    #         if action not in ['addon_jobs', 'assisted_jobs']:
    #             return jsonify({"status": "ignored"}), 200

    #         email = payment.get('email')
    #         user = get_user_data(email)
    #         user_id = int(user['user_id'])

    #         subscription = execute_query(
    #             "SELECT * FROM subscriptions WHERE user_id=%s AND status='active'",
    #             (user_id,)
    #         )[0]

    #         # Record payment
    #         self.record_payment(
    #             user_id=user_id,
    #             plan=subscription['plan'],
    #             action=action,
    #             subscription_id=subscription['id'],
    #             currency=payment['currency'],
    #             gst=0,
    #             amount=payment['amount'] / 100,
    #             method=payment.get('method'),
    #             gateway='razorpay',
    #             transaction_id=transaction_id
    #         )

    #         return jsonify({"status": "success"}), 200

    #     except Exception as e:
    #         print("[x] payment.captured error:", e)
    #         return jsonify({"status": "failed"}), 500
        
    # def handle_subscription_activated(self, payload: Dict):
    #     try:
    #         sub = payload['subscription']['entity']
    #         sub_id = sub['id']
    #         notes = sub.get('notes', {})

    #         email = notes.get('user_email')
    #         user = get_user_data(email)
    #         user_id = int(user['user_id'])

    #         # ✅ Prevent duplicates
    #         exists = execute_query(
    #             "SELECT id FROM subscriptions WHERE razorpay_subscription_id=%s",
    #             (sub_id,)
    #         )
    #         if exists:
    #             return jsonify({"status": "duplicate"}), 200

    #         start = sub['current_start']
    #         end = sub['current_end']

    #         insert_query(
    #             """
    #             INSERT INTO subscriptions
    #             (user_id, razorpay_subscription_id, plan, status, validity_start, validity_end)
    #             VALUES (%s,%s,%s,'active',FROM_UNIXTIME(%s),FROM_UNIXTIME(%s))
    #             """,
    #             (user_id, sub_id, notes.get('plan'), start, end)
    #         )

    #         update_query(
    #             "UPDATE users SET payment_status='active', is_cancelled='N' WHERE user_id=%s",
    #             (user_id,)
    #         )

    #         return jsonify({"status": "success"}), 200

    #     except Exception as e:
    #         print("[x] subscription.activated error:", e)
    #         return jsonify({"status": "failed"}), 500

    def record_subscription(self, user_id: int, plan: str, action: str, total_jobs: int,
                            assisted_job_count: int, razorpay_subscription_id: str,
                            validity_year: int, created_date: int, validity_end: int) -> int:
        """Record subscription in database"""
        query = """
            INSERT INTO subscriptions (
                user_id, plan, validity_year, validity_start, validity_end,
                jobs_allowed, remaining_jobs, assisted_jobs_allowed,
                razorpay_subscription_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
        """
        result = update_query_last_index(query, (
            user_id, plan, validity_year, created_date, validity_end,
            total_jobs, total_jobs, assisted_job_count, razorpay_subscription_id
        ))

        return result['last_index']

    def record_payment(self, user_id: int, plan: str, action: str, subscription_id: int,
                       currency: str, gst_amount: float, total_amount: float,
                       payment_method: str, gateway: str, transaction_id: str) -> int:
        """Record payment in database"""
        query = """
            INSERT INTO payments (
                user_id, plan, payment_type, subscription_id, currency,
                gst_amount, total_amount, gateway, payment_method,
                transaction_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'complete')
        """
        result = update_query_last_index(query, (
            user_id, plan, action, subscription_id, currency,
            float(gst_amount), float(total_amount), gateway, payment_method, transaction_id
        ))

        return result['last_index']

    def record_applied_promotions(self, user_id: int, notes: Dict, plan: str,
                                  action: str, payment_id: int, currency: str, payment_status: str) -> None:
        """Record applied promotions from notes"""
        promotions_json = notes.get('promotions_applied', '{}')

        if promotions_json and promotions_json != '{}':
            try:
                promotions_applied = json.loads(promotions_json)

                for promo_key, promo_data in promotions_applied.items():
                    query = """
                        INSERT INTO applied_promotions (
                            user_id, promotion_type, promotion_rule_id,
                            discount_amount, transaction_type, payment_id,
                            currency, payment_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    update_query(query, (
                        user_id,
                        promo_data.get('type', 'discount'),
                        promo_data.get('id'),
                        float(promo_data.get('discount', 0)),
                        action,
                        payment_id,
                        currency,
                        payment_status
                    ))
            except Exception as e:
                print(f"[!] Error recording promotions: {e}")

    def log_failed_payment(self, user_id: int, payment_entity: Dict) -> None:
        
        """Log failed payment attempts"""
        query = """
            INSERT INTO payments (
                user_id, payment_type, currency, total_amount,
                gateway, transaction_id, status, failure_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, 'failed', %s)
        """
        notes = payment_entity.get('notes', {})

        if notes is None:
            notes = {}

        update_query(query, (
            user_id,
            notes.get('action', 'unknown'),
            payment_entity.get('currency', 'INR').lower(),
            float(payment_entity.get('amount', 0) / 100),
            'razorpay',
            payment_entity.get('id'),
            payment_entity.get('error_description', 'Payment failed')
        ))


from flask import  jsonify
from src.models.user_authentication import api_json_response_format, get_user_data
from src.controllers.jwt_tokens.jwt_token_required import get_user_token
import json
import time
from datetime import datetime, timezone



promotion_engine = PromotionEngine()
checkout_status_manager = CheckoutStatusManager()
subscription_cancellation = SubscriptionCancellation()
stripe_processor = StripePaymentProcessor(promotion_engine)
razorpay_processor = RazorpayPaymentProcessor(promotion_engine)
stripe_webhook = StripeWebhookHandler(promotion_engine)
razorpay_webhook = RazorpayWebhookHandler(promotion_engine)



class Payment:
    


    def razorpay_create_trial(self, request):
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
                
                # recorded_subscription_id = self.record_subscription(
                #     user_id, plan_name, 'Trial', total_jobs, assisted_job_count,
                #     subscription_id or '', 0, payment_created_date
                # )
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
    
    def stripe_create_trial(self, request):
        try:
            print("check out session")
            token_result = get_user_token(request)                          
            if token_result["status_code"] == 200:  
                user_email_id = token_result["email_id"]
                user_data = get_user_data(user_email_id)
                user_role = user_data['user_role'].capitalize()
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

            pricing_key, trial_period, plan_name = '', '', ''
            # get_plan_details_query = "select attribute_value, role, plan_name from payment_config where attribute_name = %s"
            # values = (attribute_name,)
            # plan_details = execute_query(get_plan_details_query, values)
            # if len(plan_details) > 0:
            #     pricing_key = plan_details[0]['attribute_value']
            #     user_role = plan_details[0]['role']
            #     plan_name = plan_details[0]['plan_name']
            # else:
            #     return api_json_response_format("False", "Invalid attribute name", 500, {})
            # get_trial_period_query = "select attribute_value from payment_config where attribute_name = %s"

            # if user_role == 'Employer':
            #     values = ('employer_trial_period',)
            # elif user_role == 'Partner':
            #     values = ('partner_trial_period',)
            # else:
            #     values = ('professional_trial_period',)

            # trial_period_dict = execute_query(get_trial_period_query, values)
            # if len(trial_period_dict) > 0:
            #     trial_period = trial_period_dict[0]['attribute_value']
            query = "SELECT * FROM payment_plans WHERE plan = %s AND is_active = %s"
            values = ('Trialing', 1,)
            plan_details = execute_query(query, values)
            if len(plan_details) > 0:
                trial_period = plan_details[0]['trial_days']
                plan_name = plan_details[0]['plan']
            else:
                return api_json_response_format("False", "No active trial plan found", 500, {})
            
            from .payment_process_new import StripePaymentProcessor
            stripe_processor = StripePaymentProcessor(PromotionEngine())
            # create trial customer in stripe
            free_trial_product = stripe_processor.get_or_create_product('Trial Plan')
            pricing_key = stripe_processor.get_or_create_price(free_trial_product, 'Trialing', 'usd', 1)


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
                                            "metadata": { "user_email" : user_email_id, "plan" : plan_name, "type" : "subscription", "existing_plan" : "default", "new_user" : "yes"},
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
                                            metadata = {"plan": plan_name},
                                            subscription_data = {
                                                "metadata": { "user_email" : user_email_id, "plan" : plan_name, "type" : "subscription", "existing_plan" : "default", "from_trial" : "yes", "new_user" : "yes"},
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
            return api_json_response_format(False,"Something went wrong please try again",500,{})

    def get_user_checkout_details(self, request):
        """
        Get user's stored checkout details.

        """
        try:
            # Get token and user data
            token_result = get_user_token(request)
            if token_result['status_code'] != 200:
                return api_json_response_format(False, "Unauthorized", 401, {})

            user_email_id = str(token_result['email_id'])
            user_data = get_user_data(user_email_id)

            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
                
            user_id = int(user_data['user_id'])

            query = """
            SELECT 
                e.company_name as name_of_firm,
                u.first_name,
                u.last_name
            FROM 
                employer_profile e
            JOIN 
                users u ON e.employer_id = u.user_id
            WHERE 
                e.employer_id = %s;
            """
            result = execute_query(query, (user_id,))
            if not result:
                return api_json_response_format(False, "No checkout details found", 404, {})
            return api_json_response_format(True, "Checkout details retrieved", 200, result[0])

        except Exception as e:
            print(f"[x] Error fetching checkout details: {str(e)}")
            return api_json_response_format(False, "Internal server error", 500, {})
        
    def create_checkout_session(self, request):
        """
        Create checkout session for payment.

        """
        try:
            # Get token and user data
            token_result = get_user_token(request)
            if token_result['status_code'] != 200:
                return api_json_response_format(False, "Unauthorized", 401, {})

            user_email_id = str(token_result['email_id'])
            user_data = get_user_data(user_email_id)
            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
            user_id = int(user_data['user_id'])
            user_role = user_data['user_role'].capitalize()

            # Get request data
            if request.is_json:
                data = request.get_json()
            else:
                data = request.form.to_dict()

            
             # User details
            name_of_firm = data.get('name_of_firm', '')
            first_name = data.get('first_name', '')
            last_name = data.get('last_name', '')
            address = data.get('address', '')
            gst_details = data.get('gst_details', '')
            zip_code = data.get('zip_code', '')
            location = data.get('location', '')
            
            # Store user's checkout details
            chekcout_query = "INSERT INTO user_checkout_details (user_id, email_id, name_of_firm, first_name, last_name, address, location, zip_code, gst_details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name_of_firm = %s, first_name = %s, last_name = %s, address = %s, location = %s, zip_code = %s, gst_details = %s"
            checkout_values = (user_id, user_email_id, name_of_firm, first_name, last_name, address, location, zip_code, gst_details, name_of_firm, first_name, last_name, address, location, zip_code, gst_details)
            update_query(chekcout_query,checkout_values)

            print(f"[*] Stored checkout details for user_id: {user_id}")

            # Plan details
            plan_name = data.get('plan', '').capitalize()
            years = int(data.get('years', 1))
            currency = data.get('currency', 'inr').lower()
            # currency = 'inr'
            gateway = data.get('gateway', '').lower()
            # plan_name = 'Basic'
            # gateway = 'razorpay'
            coupon_code = data.get('coupon_code', '')
            assisted_job_count = int(data.get('assisted_job_count', 0))
            addon_job_count = int(data.get('addon_job_count', 0))

            # print debug info
            print(f"[*] Creating checkout session for user_id: {user_id}")
            print(f"    Plan: {plan_name}, Years: {years}, Currency: {currency}, Gateway: {gateway}")

            # Validate gateway
            if gateway not in ['stripe', 'razorpay']:
                return api_json_response_format(False, "Invalid payment gateway", 400, {})

            # Get plan details
            plan_query = "SELECT * FROM payment_plans WHERE plan = %s"
            plan_result = execute_query(plan_query, (plan_name,))

            if not plan_result:
                return api_json_response_format(False, f"Plan '{plan_name}' not found", 404, {})

            plan_data = plan_result[0]
            base_price = float(plan_data['base_price_inr']) if currency == 'inr' else float(plan_data['base_price_usd'])
            total_jobs = plan_data['total_jobs'] * years

            # Determine transaction type
            subscription_query = "SELECT * FROM subscriptions WHERE user_id = %s AND status = 'active' ORDER BY id DESC LIMIT 1"
            subscription_result = execute_query(subscription_query, (user_id,))

            user_subscription_query = "SELECT pricing_category, current_period_end, current_period_start, payment_status, is_cancelled FROM users WHERE user_id = %s"
            user_subscription_result = execute_query(user_subscription_query, (user_id,))

            current_plan = None
            transaction_type = 'new_plan_subscription'

            current_time = int(time.time())

            payment_status = None
            validity_end = None
            is_cancelled = 'N'

            if not subscription_result:
                if user_subscription_result:
                    current_plan = user_subscription_result[0]['pricing_category']
                    validity_end = user_subscription_result[0]['current_period_end']
                    validity_start = user_subscription_result[0]['current_period_start']
            else:
                current_plan = subscription_result[0]['plan']
                validity_end = subscription_result[0]['validity_end']
                validity_start = subscription_result[0]['validity_start']

            is_cancelled = user_subscription_result[0]['is_cancelled']

            payment_status = user_subscription_result[0]['payment_status'] if user_subscription_result else None
            current_plan = 'Trial' if payment_status in ['trialing', 'trial_expired'] else current_plan

            is_checkout_page = False

            if name_of_firm and first_name and last_name:
                print(f"[*] Checkout details - Firm: {name_of_firm}, Name: {first_name} {last_name}")
                is_checkout_page = True

            if current_plan is None or payment_status == 'cancelled':
                transaction_type = 'new_plan_subscription'
            elif current_plan == plan_name and payment_status == 'active':
                if validity_end is not None and current_time > validity_end and plan_name != 'Basic':
                    # transaction_type = 'renewal'
                    print(f"[*] Subscription expired for user_id: {user_id}")
                    return api_json_response_format(False, "Your current subscription has expired. Please renew or choose a different plan.", 400, {})
                # elif is_cancelled == 'Y' and assisted_job_count >= 0:
                #     transaction_type = 'new_plan_subscription'
                elif is_cancelled == 'Y' and is_checkout_page:
                    transaction_type = 'new_plan_subscription'
                elif addon_job_count > 0:
                    transaction_type = 'addon_jobs'
                elif assisted_job_count > 0:
                    transaction_type = 'assisted_jobs'
                else:
                    transaction_type = 'same_plan_no_change'
                    return api_json_response_format(False, "You are already on this plan", 400, {})
            else:
                transaction_type = 'upgrade' if self.is_upgrade(current_plan, plan_name) else 'downgrade'

            is_new_subscriber = subscription_result is None or transaction_type == 'new_plan_subscription'
            
            # transaction_type = 'addon_jobs'
            if transaction_type == 'addon_jobs' and addon_job_count > 0 and payment_status == 'active' and is_cancelled == 'Y' and validity_end is not None and current_time > validity_end:
                print(f"[*] Cannot add addon jobs to a cancelled subscription for user_id: {user_id}")
                return api_json_response_format(False, "Cannot add addon jobs to a cancelled subscription. Please buy/renew a plan first.", 400, {})
        
            if transaction_type == 'assisted_jobs' and assisted_job_count > 0:
                query = "SELECT assisted_jobs_allowed, assisted_jobs_used, no_of_jobs, total_jobs, additional_jobs_count FROM user_plan_details WHERE user_id = %s"
                result = execute_query(query, (user_id,))
                if result:
                    assisted_jobs_allowed = result[0]['assisted_jobs_allowed']
                    assisted_jobs_used = result[0]['assisted_jobs_used']
                    no_of_jobs = result[0]['no_of_jobs']
                    total_jobs = result[0]['total_jobs']
                    additional_jobs_count = result[0]['additional_jobs_count']

                    validity_start_ts = validity_start
                    validity_end_ts = validity_end

                    validity_start_assist = datetime.fromtimestamp(validity_start_ts)
                    validity_end_assist = datetime.fromtimestamp(validity_end_ts)

                    validity_assist_years = int(
                            round((validity_end_assist - validity_start_assist).days / 365.25)
                        )

                    validity_assist_years = int(validity_assist_years)

                    print(validity_start_assist)
                    print(validity_end_assist)
                    print(validity_assist_years)
                    # if current_plan in ['Basic', 'Premium'] and currency == 'usd' and gateway == 'stripe':
                    #     if (assisted_jobs_allowed + assisted_job_count) > (2 * validity_assist_years):
                    #         print(f"[*] Assisted job limit exceeded for user_id: {user_id}")
                    #         return api_json_response_format(False, "Assisted job limit exceeded.", 400, {})
                    
                    # elif current_plan == 'Platinum' and currency == 'usd' and gateway == 'stripe':
                        
                    #     if (assisted_job_count + assisted_jobs_allowed) > (4 * validity_assist_years):
                    #         print(f"[*] Assisted job limit exceeded for user_id: {user_id}")
                    #         return api_json_response_format(False, "Assisted job limit exceeded.", 400, {}) 

                    # if ((assisted_job_count + assisted_jobs_allowed) > (total_jobs + additional_jobs_count)) and currency == 'inr' and gateway == 'razorpay':
                    #     print(f"[*] Assisted job limit exceeded for user_id: {user_id}")
                    #     return api_json_response_format(False, "Assisted job limit exceeded.", 400, {})
                    
            print(f"[*] Subscriber: {'NEW' if is_new_subscriber else 'EXISTING'}")
            print(f"[*] Transaction type: {transaction_type}")
            print(f"[*] Current plan: {current_plan}\n")

            subtotal_before_discounts = 0

            # Only add base amount for NEW subscribers
            if is_new_subscriber:
                subtotal_before_discounts = base_price * years
                print(f"[*] Base price: {base_price} × {years} years = {subtotal_before_discounts}")
            elif not is_new_subscriber and transaction_type in ['upgrade', 'downgrade']:
                subtotal_before_discounts = base_price * years
                print(f"[*] Existing subscriber - base price for {transaction_type}: {base_price} × {years} years = {subtotal_before_discounts}")
            else:
                print(f"[*] Existing subscriber - base NOT added")

            # Add assisted jobs (for new or existing)
            assisted_job_price = 0
            if assisted_job_count > 0:
                assisted_job_price = promotion_engine.calculate_assisted_job_amount(
                    plan_name, currency, assisted_job_count
                )
                # subtotal_before_discounts += assisted_job_price
                print(f"[*] Assisted jobs ({assisted_job_count}): +{assisted_job_price}")

            # Add addon jobs (for new or existing)
            addon_job_price = 0
            if addon_job_count > 0:
                addon_job_price = promotion_engine.calculate_addon_job_amount(
                    plan_name, currency, addon_job_count
                )
                # subtotal_before_discounts += addon_job_price
                print(f"[*] Addon jobs ({addon_job_count}): +{addon_job_price}")

            print(f"[*] Subtotal before discounts: {subtotal_before_discounts}\n")

            
            context = PromotionContext(
                user_id=user_id,
                user_role=user_role,
                plan_name=plan_name,
                subscription_plan=plan_data['id'],
                subscription_id=subscription_result[0]['id'] if subscription_result else None,
                transaction_type=transaction_type,
                original_amount=addon_job_price if addon_job_count >= 1 and transaction_type == 'addon_jobs' else subtotal_before_discounts,
                currency=currency,
                coupon_code=coupon_code,
                years=years,
                assisted_job_count=assisted_job_count,
                current_plan=current_plan,
                addon_count=addon_job_count,
                validity_end=validity_end
            )
            
            promotions = promotion_engine.calculate_all_promotions(context)

            
            total_discount_amount = 0
            discount_breakdown = {}

            
            promotions_to_apply = [
                ('signup_discount', promotions.get('signup_discount')),
                ('yearly_discount', promotions.get('yearly_discount')),
                ('upgrade_discount', promotions.get('upgrade_discount')),
                ('addon_discount', promotions.get('addon_discount')),
                ('coupon_discount', promotions.get('coupon_discount')),
                ('downgrade_discount', promotions.get('downgrade_discount'))
            ]

            print("[*] Applying promotions:")

            for promo_name, promo_obj in promotions_to_apply:
                if promo_obj and promo_obj.discount_amount > 0:
                    discount_amount = float(promo_obj.discount_amount)
                    if promo_name == 'addon_discount':
                        addon_job_price -= discount_amount
                        # total_discount_amount -= discount_amount
                    elif promo_name == 'assisted_job_discount':
                        assisted_job_price -= discount_amount  
                        # total_discount_amount -= discount_amount
                    else:
                        total_discount_amount += discount_amount
                    discount_breakdown[promo_name] = discount_amount
                    print(f"    {promo_name}: -{discount_amount}")

            if total_discount_amount == 0:
                print(f"    (no promotions applied)")

            print(f"[*] Total discount: {total_discount_amount}\n")

            
            subtotal_after_discounts = subtotal_before_discounts - total_discount_amount + addon_job_price + assisted_job_price
            # subtotal_after_discounts = subtotal_before_discounts + total_discount_amount

            if subtotal_after_discounts < 0:
                subtotal_after_discounts = 0
                print(f"[!] Warning: Discount exceeds subtotal, setting to 0")

            print(f"[*] Subtotal after discounts: {subtotal_after_discounts}")

            assisted_job_count_by_user = assisted_job_count
            if is_new_subscriber and plan_name == 'Platinum' and assisted_job_count > 0:
                assisted_job_count = (plan_data['assisted_jobs'] * years) + assisted_job_count

            elif is_new_subscriber and plan_name == 'Platinum' and assisted_job_count == 0:
                assisted_job_count = plan_data['assisted_jobs'] * years

            elif not is_new_subscriber and plan_name == 'Platinum' and (transaction_type == 'upgrade' or transaction_type == 'new_plan_subscription') and assisted_job_count > 0:
                assisted_job_count = (plan_data['assisted_jobs'] * years) + assisted_job_count
            
            elif not is_new_subscriber and plan_name == 'Platinum' and (transaction_type == 'upgrade' or transaction_type == 'new_plan_subscription') and assisted_job_count == 0:
                assisted_job_count = plan_data['assisted_jobs'] * years
            print(f"[*] Final assisted job count: {assisted_job_count}\n")
            gst_amount = 0
            if currency == 'inr':
                gst_amount = round(subtotal_after_discounts * 0.18, 2)
            total_amount = round(subtotal_after_discounts + gst_amount, 2)

            print(f"[*] GST (18%): {gst_amount}")
            print(f"[*] TOTAL AMOUNT: {total_amount}\n")

        
            metadata = {
                # User & plan info
                "user_email": user_email_id,
                "plan": plan_name,
                "current_plan": current_plan,
                "years": int(years),
                "action": transaction_type,
                "currency": currency,
                "subscriber_type": "new" if is_new_subscriber else "existing",
                
                "gst_amount": float(gst_amount),
                "total_amount": float(total_amount),
                
                # Job info
                "total_jobs": int(total_jobs),
                "assisted_job_count": int(assisted_job_count),
                "addon_job_count": int(addon_job_count),
                "coupon_code": coupon_code or "",
                
                # Promotions breakdown
                "promotions_applied": json.dumps({
                    key: {
                        'id': value.promotion_id,
                        'type': value.promotion_type,
                        'discount_type': value.discount_type,
                        'discount_value': float(value.discount_value),
                        'discount_amount': float(value.discount_amount)
                    } for key, value in promotions.items() if value is not None
                })
            }

            
            # Determine payment method
            payment_method = 'one-time' if plan_name in ['Basic', 'Trialing'] or transaction_type in ['addon_jobs', 'assisted_jobs'] else 'subscription'

            country_code = user_data['country_code']
            contact_number = user_data['contact_number']
            phone_number = f"+{country_code}{contact_number}" if country_code and contact_number else None
            
            # Create checkout session
            if gateway == 'stripe':
                customer_id = stripe_processor.get_or_create_customer(user_email_id, user_id)
                return stripe_processor.create_checkout_session(
                    context, customer_id, plan_name, total_amount, currency,
                    years, assisted_job_count_by_user, metadata, user_role, user_email_id, payment_method
                )

            elif gateway == 'razorpay':
                customer_id = razorpay_processor.get_or_create_customer(user_email_id, user_id)
                return razorpay_processor.create_checkout_session(context, customer_id, plan_name, total_amount, currency, years, assisted_job_count_by_user, metadata, user_email_id, phone_number, payment_method, 7)
        except Exception as e:
            print(f"[x] Error creating checkout session: {str(e)}")
            return api_json_response_format(False, str(e), 500, {})



    def get_checkout_status(self, request):
        """Get current checkout/subscription status"""
        return checkout_status_manager.get_checkout_status(request)


    

    def cancel_subscription(self, request):
        """
        Cancel active subscription
        
        """
        try:
            token_result = get_user_token(request)
            if token_result['status_code'] != 200:
                return api_json_response_format(False, "Unauthorized", 401, {})

            user_email_id = str(token_result['email_id'])
            user_data = get_user_data(user_email_id)
            flag_sub_user = False
            if not user_data['is_exist']:
                user_data = get_sub_user_data(user_email_id)
                flag_sub_user = True

            user_id = int(user_data['user_id'])
            user_email_id_list = []
            if flag_sub_user:
                get_owner_of_sub_user_query = "SELECT user_id FROM sub_users WHERE email_id = %s"
                owner_result = execute_query(get_owner_of_sub_user_query, (user_email_id,))
                if owner_result:
                    owner_user_id = int(owner_result[0]['user_id'])
                else:
                    owner_user_id = 0
                get_owner_email_id_query = "SELECT email_id FROM users WHERE user_id = %s"
                owner_email_result = execute_query(get_owner_email_id_query, (owner_user_id,))
                if owner_email_result:
                    owner_email_id = str(owner_email_result[0]['email_id'])
                    user_id = owner_user_id
                else:
                    owner_email_id = ''
                user_email_id_list.append(user_email_id)
                user_email_id_list.append(owner_email_id)
            else:
                user_email_id_list.append(user_email_id)

            stripe_query = "SELECT * FROM stripe_customers WHERE email IN %s"
            stripe_result = execute_query(stripe_query, (user_email_id_list,))

            if not stripe_result:
                gateway = 'razorpay'
            else:
                gateway = 'stripe'

            # return api_json_response_format(False, "Subscription cancellation is currently disabled for maintenance.", 503, gateway)

            # data = request.get_json() if request.is_json else request.form.to_dict()
            # gateway = data.get('gateway', 'stripe').lower()

            result = subscription_cancellation.cancel_subscription(user_id, gateway, user_email_id)

            if result['success']:
                return api_json_response_format(True, result['message'], 200, {})
            else:
                return api_json_response_format(False, result['message'], 400, {})

        except Exception as e:
            print(f"[x] Error cancelling subscription: {str(e)}")
            return api_json_response_format(False, str(e), 500, {})


    def stripe_webhook_endpoint(self, request):
        """Webhook endpoint for Stripe events"""
        return stripe_webhook.handle_webhook(request)


    def razorpay_webhook_endpoint(self, request):
        """Webhook endpoint for Razorpay events"""
        return razorpay_webhook.handle_webhook(request)

    
    def is_upgrade(self, current_plan: str, new_plan: str) -> bool:
        """Check if plan change is an upgrade"""
        plan_hierarchy = {'None': -1, 'Trial': 0, 'Basic': 1, 'Premium': 2, 'Platinum': 3}
        return plan_hierarchy.get(new_plan, 0) > plan_hierarchy.get(current_plan, 0)



