SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) src PIVOT (MAX(monthly_fee) AS value_sum, COUNT(1) AS cnt FOR status IN ('CANCELLED', 'EXPIRED', 'ACTIVE'))
