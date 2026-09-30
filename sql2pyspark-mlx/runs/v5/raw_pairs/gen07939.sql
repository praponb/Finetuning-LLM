select id, paid_at from payments where paid_at >= '2024-10-01' or to_date(paid_at) < '2025-09-01';
