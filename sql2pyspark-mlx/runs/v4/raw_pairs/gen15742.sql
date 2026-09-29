select status, max(amount) as max_amount, count(amount) as amount_count from payments where order_id = 5 or paid_at < '2024-08-01' group by status order by max_amount desc;
