with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        cost,
        date_trunc('month', delivered_date) as delivered_date_period,
        coalesce(carrier, 'NONE') as carrier_filled
    from shipments
    where shipped_date <= '2024-08-18'
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', delivered_date_period) as quarter_start,
    (unix_timestamp(max(delivered_date_period)) - unix_timestamp(min(delivered_date_period))) / 60 as span_minutes,
    approx_percentile(cost, 0.5) as cost_p50,
    percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    round(avg(cost), 2) as avg_cost
from cleaned
group by date_trunc('quarter', delivered_date_period)
