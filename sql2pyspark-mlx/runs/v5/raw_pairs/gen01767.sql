SELECT id, store_id, units, QUARTER(sale_date) sale_date_quarter FROM store_sales WHERE units BETWEEN 13 AND 30 AND sale_date > '2025-06-02' AND product_id IN (1, 2, 4, 6)
