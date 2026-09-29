select * from (select customer_id, status, monthly_fee from subscriptions) src pivot (avg(monthly_fee) for status in ('EXPIRED', 'PAUSED', 'CANCELLED'))
