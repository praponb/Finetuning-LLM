SELECT shipped_date, status FROM shipments WHERE YEAR(delivered_date) = 2023 EXCEPT SELECT shipped_date, status FROM shipments WHERE cost >= 54
