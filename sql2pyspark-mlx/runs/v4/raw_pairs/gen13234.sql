with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(shipped_date, delivered_date) as days_delivered_date_to_shipped_date,
        coalesce(carrier, 'UNKNOWN') as carrier_filled
    from shipments
    where shipped_date > date '2023-09-15'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(cost) over (partition by carrier order by delivered_date, id rows between 2 preceding and 1 preceding) as cost_avg_2_prior,
        row_number() over w_recent as seq_no
    from base
    window w_recent as (partition by status order by cost desc, id)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from with_windows
    qualify row_number() over (partition by order_id order by days_delivered_date_to_shipped_date desc nulls last, id) = 1
)
-- Native SQL PIVOT
select *
from (
    select
        order_id,
        status,
        cost_avg_2_prior
    from latest
) src
pivot (
    max(cost_avg_2_prior)
    for status in ('LOST', 'IN_TRANSIT')
);
