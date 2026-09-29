select status, case when monthly_fee >= 254 then 'high' when monthly_fee >= 68 then 'mid' else 'low' end as monthly_fee_bucket, min(start_date) as first_start_date
from subscriptions
where start_date > '2024-01-18'
group by 1, 2
having count(*) > 5
