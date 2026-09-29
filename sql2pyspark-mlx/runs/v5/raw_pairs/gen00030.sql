select avg(monthly_fee) as avg_monthly_fee, round((100 * sum(case when status = 'ACTIVE' then 1 else 0 end)) / count(*), 1) as pct_active from subscriptions order by avg_monthly_fee desc
