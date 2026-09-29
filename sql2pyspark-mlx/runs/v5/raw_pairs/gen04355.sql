select * from (select plan, status, monthly_fee from subscriptions) pivot (sum(monthly_fee) for status in ('EXPIRED', 'ACTIVE')) order by plan;
