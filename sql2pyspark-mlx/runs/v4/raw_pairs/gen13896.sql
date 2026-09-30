SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS LAST, id) AS latest_rank FROM payments QUALIFY latest_rank = 1
