SELECT status, DATE_TRUNC('MONTH', delivered_date) delivered_month, MIN(cost) min_cost
FROM shipments
GROUP BY 1, 2
