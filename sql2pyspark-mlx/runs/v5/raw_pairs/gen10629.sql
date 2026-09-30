SELECT MIN(cost) min_cost FROM shipments WHERE MONTH(shipped_date) IN (9, 10, 11) AND cost BETWEEN 47 AND 84;
