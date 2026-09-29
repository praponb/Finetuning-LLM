WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS row_num FROM accounts WHERE customer_id IN (6, 7, 25)) SELECT * FROM numbered WHERE row_num <= 1;
