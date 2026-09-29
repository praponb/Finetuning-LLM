WITH ranked AS (SELECT id, method, paid_at, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rn FROM payments) SELECT id, method, paid_at FROM ranked WHERE rn <= 5
