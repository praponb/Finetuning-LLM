with cleaned as (
    select *, date_format(end_date, 'yyyy-MM') as end_date_label
    from subscriptions
    where end_date > to_date('2024-05-01')
      and customer_id is not null
)
select
    date_trunc('year', end_date) as year_start,
    percentile_approx(monthly_fee, array(0.25, 0.5, 0.75)) as monthly_fee_quartiles,
    approx_percentile(monthly_fee, 0.5) as monthly_fee_p50,
    count(case when status = 'EXPIRED' then 1 end) as expired_count
from cleaned
group by 1
having approx_percentile(monthly_fee, 0.5) >= 0
order by expired_count;
