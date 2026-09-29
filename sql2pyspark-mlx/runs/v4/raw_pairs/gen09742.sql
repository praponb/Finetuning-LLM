SELECT order_id, LAST_DAY(delivered_date) AS delivered_date_month_end, QUARTER(shipped_date) AS shipped_date_quarter
FROM shipments
WHERE NOT (order_id = 10 AND (shipped_date >= '2023-08-08' OR order_id = 38))
ORDER BY 3 DESC, delivered_date_month_end;
