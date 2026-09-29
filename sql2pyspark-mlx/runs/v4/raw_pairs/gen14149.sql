select id, status, round(amount, 0) as amount_rounded from payments where paid_at < '2024-11-01';
