SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY opened_date DESC, id) AS rn FROM accounts QUALIFY rn = 1;
