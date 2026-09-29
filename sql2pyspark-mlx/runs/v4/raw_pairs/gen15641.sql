select * from (select month(start_date) as start_month, status, monthly_fee from subscriptions) pivot (max(monthly_fee) for status in ('CANCELLED', 'ACTIVE', 'EXPIRED')) order by start_month
