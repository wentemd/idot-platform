"""
Stripe payment and subscription routes
"""
from fastapi import APIRouter, HTTPException, Request, Header
from fastapi.responses import RedirectResponse
import stripe
import os
from datetime import datetime

from app.api.users import get_user_by_id, update_user, get_user_by_token
from app.api.auth import get_current_user

router = APIRouter()

# Stripe configuration
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "")  # Your Pro plan price ID
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://idot-platform.onrender.com")

# Price configuration
PRO_MONTHLY_PRICE = 4900  # $49.00 in cents
PRO_YEARLY_PRICE = 49900  # $499.00 in cents (save $89)


@router.post("/create-checkout-session")
async def create_checkout_session(request: Request, plan: str = "monthly"):
    """Create a Stripe checkout session for Pro subscription"""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Must be logged in to subscribe")
    
    if not stripe.api_key:
        raise HTTPException(status_code=500, detail="Stripe not configured")
    
    # Get or create Stripe customer
    if user['stripe_customer_id']:
        customer_id = user['stripe_customer_id']
    else:
        customer = stripe.Customer.create(
            email=user['email'],
            name=user['name'],
            metadata={"user_id": user['id']}
        )
        customer_id = customer.id
        update_user(user['id'], stripe_customer_id=customer_id)
    
    # Determine price
    if plan == "yearly":
        price_id = os.getenv("STRIPE_YEARLY_PRICE_ID", STRIPE_PRICE_ID)
    else:
        price_id = os.getenv("STRIPE_MONTHLY_PRICE_ID", STRIPE_PRICE_ID)
    
    if not price_id:
        raise HTTPException(status_code=500, detail="Stripe price not configured")
    
    try:
        checkout_session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=[{
                "price": price_id,
                "quantity": 1
            }],
            mode="subscription",
            subscription_data={
                "trial_period_days": 7  # 7-day free trial
            },
            success_url=f"{FRONTEND_URL}?subscription=success",
            cancel_url=f"{FRONTEND_URL}?subscription=cancelled",
            metadata={"user_id": user['id']}
        )
        
        return {"checkout_url": checkout_session.url}
    
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/create-portal-session")
async def create_portal_session(request: Request):
    """Create a Stripe customer portal session for managing subscription"""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Must be logged in")
    
    if not user['stripe_customer_id']:
        raise HTTPException(status_code=400, detail="No subscription found")
    
    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=user['stripe_customer_id'],
            return_url=f"{FRONTEND_URL}?portal=closed"
        )
        
        return {"portal_url": portal_session.url}
    
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhooks"""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    
    if not STRIPE_WEBHOOK_SECRET:
        # In development, skip signature verification
        event = stripe.Event.construct_from(
            payload, stripe.api_key
        )
    else:
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Invalid signature")
    
    # Handle the event
    event_type = event["type"]
    data = event["data"]["object"]
    
    if event_type == "checkout.session.completed":
        # Payment successful, activate subscription
        handle_checkout_completed(data)
    
    elif event_type == "customer.subscription.updated":
        handle_subscription_updated(data)
    
    elif event_type == "customer.subscription.deleted":
        handle_subscription_deleted(data)
    
    elif event_type == "invoice.payment_failed":
        handle_payment_failed(data)
    
    return {"status": "success"}


def handle_checkout_completed(session):
    """Handle successful checkout"""
    customer_id = session.get("customer")
    subscription_id = session.get("subscription")
    
    # Find user by Stripe customer ID
    from app.api.users import get_user_db
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE stripe_customer_id = ?", [customer_id])
    row = cursor.fetchone()
    conn.close()
    
    if row:
        # Get subscription details
        subscription = stripe.Subscription.retrieve(subscription_id)
        end_date = datetime.fromtimestamp(subscription.current_period_end).isoformat()
        
        update_user(
            row['id'],
            tier='pro',
            stripe_subscription_id=subscription_id,
            subscription_status='active',
            subscription_end_date=end_date
        )


def handle_subscription_updated(subscription):
    """Handle subscription updates"""
    customer_id = subscription.get("customer")
    status = subscription.get("status")
    
    # Find user
    from app.api.users import get_user_db
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE stripe_customer_id = ?", [customer_id])
    row = cursor.fetchone()
    conn.close()
    
    if row:
        tier = 'pro' if status in ['active', 'trialing'] else 'free'
        end_date = None
        if subscription.get("current_period_end"):
            end_date = datetime.fromtimestamp(subscription["current_period_end"]).isoformat()
        
        update_user(
            row['id'],
            tier=tier,
            subscription_status=status,
            subscription_end_date=end_date
        )


def handle_subscription_deleted(subscription):
    """Handle subscription cancellation"""
    customer_id = subscription.get("customer")
    
    # Find user
    from app.api.users import get_user_db
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE stripe_customer_id = ?", [customer_id])
    row = cursor.fetchone()
    conn.close()
    
    if row:
        update_user(
            row['id'],
            tier='free',
            subscription_status='cancelled',
            stripe_subscription_id=None
        )


def handle_payment_failed(invoice):
    """Handle failed payment"""
    customer_id = invoice.get("customer")
    
    # Find user
    from app.api.users import get_user_db
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE stripe_customer_id = ?", [customer_id])
    row = cursor.fetchone()
    conn.close()
    
    if row:
        update_user(
            row['id'],
            subscription_status='past_due'
        )


@router.get("/subscription-status")
async def get_subscription_status(request: Request):
    """Get current user's subscription status"""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    return {
        "tier": user['tier'],
        "subscription_status": user['subscription_status'],
        "subscription_end_date": user['subscription_end_date'],
        "stripe_customer_id": user['stripe_customer_id'] is not None
    }
