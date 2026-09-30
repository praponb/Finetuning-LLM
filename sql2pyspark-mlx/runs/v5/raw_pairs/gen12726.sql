SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC, id) AS rank_in_group
FROM payments
QUALIFY rank_in_group = 1;
