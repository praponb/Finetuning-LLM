SELECT plan, COUNT(DISTINCT monthly_fee) AS unique_statuss FROM subscriptions GROUP BY plan ORDER BY unique_statuss DESC, plan;
