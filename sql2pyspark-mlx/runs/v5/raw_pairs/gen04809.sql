SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id) AS row_num
FROM accounts
QUALIFY row_num = 1;
