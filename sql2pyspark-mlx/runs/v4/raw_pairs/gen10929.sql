select * from (select status, plan, monthly_fee from subscriptions where end_date < '2025-04-01')
pivot (count(monthly_fee) for plan in ('standard', 'basic', 'premium'))
order by status
