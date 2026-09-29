SELECT id, customer_id FROM accounts WHERE customer_id IN (14, 19, 21, 28) UNION ALL SELECT id, customer_id FROM accounts WHERE opened_date <= '2025-09-02' ORDER BY customer_id;
