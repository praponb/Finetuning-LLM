SELECT id, method, paid_at, PERCENT_RANK() OVER (PARTITION BY method ORDER BY paid_at) AS paid_at_pct_rank FROM payments WHERE method IS NOT NULL ORDER BY 1;
