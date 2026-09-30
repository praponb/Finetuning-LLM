SELECT monthly_fee AS value FROM subscriptions UNION SELECT amount AS value FROM transactions WHERE amount > 1900;
