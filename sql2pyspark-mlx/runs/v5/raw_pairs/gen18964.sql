SELECT QUARTER(last_restock) AS last_restock_quarter, COUNT(DISTINCT quantity) AS unique_stores, AVG(quantity) AS avg_quantity FROM inventory GROUP BY QUARTER(last_restock)
