SELECT monthly_fee FROM subscriptions EXCEPT SELECT amount FROM transactions WHERE merchant NOT IN ('7-Eleven', 'Grab', 'Lazada')
