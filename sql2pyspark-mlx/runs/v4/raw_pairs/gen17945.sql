select * from (select status, plan, monthly_fee from subscriptions)
pivot (avg(monthly_fee) for plan in ('basic', 'premium', 'standard', 'enterprise'))
order by status
