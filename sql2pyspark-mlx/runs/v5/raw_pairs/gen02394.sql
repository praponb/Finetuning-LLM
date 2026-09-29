SELECT units, store_id, revenue, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label FROM store_sales WHERE revenue <= 2500 AND store_id IN (4, 6) AND revenue BETWEEN 1200 AND 2800;
