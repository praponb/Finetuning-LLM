SELECT monthly_fee value FROM subscriptions EXCEPT SELECT amount value FROM payments;
