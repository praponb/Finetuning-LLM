with base as (
    select
        id,
        method,
        amount,
        paid_at,
        trim(status) as clean_status,
        coalesce(method, 'DIRECT') as method_clean
    from payments
    where paid_at > date '2024-01-30'
      and order_id is not null
)
select
    date_trunc('month', paid_at) as activity_month,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    count_if(method = 'card') as card_events,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    count(*) as num_rows,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as span_minutes
from base
group by activity_month
