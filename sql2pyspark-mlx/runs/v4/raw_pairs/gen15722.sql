SELECT * FROM (SELECT DATE_TRUNC('MONTH', delivered_date) AS delivered_month, carrier, cost FROM shipments) PIVOT (MAX(cost) FOR carrier IN ('Kerry', 'UPS', 'Flash', 'DHL')) ORDER BY delivered_month
