SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC NULLS FIRST, id) AS rank_in_group
FROM transactions
QUALIFY rank_in_group = 1
