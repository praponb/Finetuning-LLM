SELECT a.id, a.opened_date, a.customer_id, c.segment, c.age, REGEXP_REPLACE(c.name, ' ', '-') AS name_slug FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.credit_limit <= 8300;
