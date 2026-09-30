SELECT id, txn_time, is_flagged FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id ASC) AS dedup_rank FROM transactions) t WHERE dedup_rank = 1
