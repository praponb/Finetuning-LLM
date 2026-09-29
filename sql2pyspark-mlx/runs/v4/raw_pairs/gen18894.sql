SELECT monthly_fee AS value FROM subscriptions WHERE monthly_fee < 150 INTERSECT SELECT amount AS value FROM payments
