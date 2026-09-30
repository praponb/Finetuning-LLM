SELECT status, carrier, ROUND(cost * 0.9, 2) AS cost_scaled FROM shipments WHERE YEAR(shipped_date) >= 2025
