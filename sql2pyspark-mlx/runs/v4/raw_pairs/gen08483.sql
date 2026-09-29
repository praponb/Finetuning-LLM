with base as (select * from subscriptions where status = 'CANCELLED'), agg as (select plan, max(monthly_fee) - min(monthly_fee) as monthly_fee_range from base group by plan) select * from agg
