select * from (select plan, status, monthly_fee from subscriptions)
pivot (count(monthly_fee) for status in ('PAUSED', 'CANCELLED', 'ACTIVE', 'EXPIRED'))
order by plan;
