WITH RankedPayments AS (SELECT *, RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) AS row_num FROM payments) SELECT id, method, paid_at, status FROM RankedPayments WHERE row_num <= 3
