select * from (select plan, status, monthly_fee from subscriptions) pivot (sum(monthly_fee) for status in ('ACTIVE', 'CANCELLED')) order by plan
