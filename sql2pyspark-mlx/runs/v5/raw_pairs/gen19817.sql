SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS amount_rank
FROM payments
ORDER BY amount_rank DESC, id;
