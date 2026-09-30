SELECT status FROM payments WHERE status = 'REFUNDED' EXCEPT SELECT status FROM subscriptions WHERE plan <> 'premium'
