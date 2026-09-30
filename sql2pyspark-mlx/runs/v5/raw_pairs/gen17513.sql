SELECT *, RANK() OVER (PARTITION BY method, status ORDER BY paid_at DESC NULLS LAST) AS rank_in_group
FROM payments
QUALIFY rank_in_group = 1
