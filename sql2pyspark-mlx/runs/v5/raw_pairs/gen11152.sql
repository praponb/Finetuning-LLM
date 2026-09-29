with deduped as (
    select *
    from payments
    qualify row_number() over (partition by status order by amount desc nulls last, id) = 1
)
select
    date_trunc('year', paid_at) as year_start,
    order_id,
    stddev_samp(amount) as amount_stddev,
    percentile_approx(amount, 0.95) as amount_p95,
    count(*) filter (where status = 'FAILED') as failed_rows
from deduped
group by year_start, order_id
having amount_p95 >= 0
order by order_id desc nulls first;
