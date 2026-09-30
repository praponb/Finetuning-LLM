SELECT id, status, amount, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS amount_row FROM payments;
