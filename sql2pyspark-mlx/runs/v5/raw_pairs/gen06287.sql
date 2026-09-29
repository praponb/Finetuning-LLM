with with_windows as (
    -- Ranking and gap features
    select
        *,
        lag(units) over (partition by product_id order by units, id) as prev_units,
        lead(units) over (partition by product_id order by units, id) as next_units,
        lead(revenue) over (partition by product_id order by units, id) as next_revenue,
        lag(revenue) over (partition by product_id order by units, id) as prev_revenue,
        row_number() over (partition by product_id order by units, id) as event_seq
    from store_sales
),
with_windows_store_sales as (
    -- Running and moving aggregates
    select
        id,
        units,
        prev_units,
        next_revenue,
        event_seq,
        lead(next_units) over (partition by store_id order by sale_date, id) as next_next_units,
        lag(next_units) over (partition by store_id order by sale_date, id) as prev_next_units,
        lead(event_seq) over (partition by store_id order by sale_date, id) as next_event_seq,
        lag(event_seq) over (partition by store_id order by sale_date, id) as prev_event_seq,
        row_number() over (partition by store_id order by sale_date, id) as row_seq,
        avg(prev_units) over (partition by store_id order by revenue, id rows between 2 preceding and 1 preceding) as prev_units_avg_2_prior
    from with_windows
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, prev_units + prev_units_avg_2_prior as prev_units_plus_prev_units_avg_2_prior
    from with_windows_store_sales
    where prev_event_seq = 4
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over w_running as units_bucket,
        sum(next_next_units) over (order by prev_units, id rows between unbounded preceding and current row) as running_next_next_units,
        cume_dist() over (order by next_revenue nulls first) as next_revenue_cume_dist
    from filtered
    window w_running as (order by units desc, id)
),
enriched_store_sales as (
    -- Running and moving aggregates
    select
        id,
        prev_units,
        next_revenue,
        next_next_units,
        prev_next_units,
        next_event_seq,
        row_seq,
        prev_units_plus_prev_units_avg_2_prior,
        units_bucket,
        running_next_next_units,
        next_revenue_cume_dist,
        last_value(next_next_units, true) over w_hist as last_known_next_next_units,
        first_value(event_seq) ignore nulls over (order by row_seq, id rows between unbounded preceding and current row) as first_known_event_seq,
        last_value(prev_next_units) ignore nulls over w_hist as last_known_prev_next_units
    from enriched
    where prev_units = 18
    window w_hist as (order by prev_next_units, id rows between unbounded preceding and current row)
)
select
    id,
    next_revenue,
    next_next_units,
    prev_next_units,
    next_event_seq,
    row_seq,
    prev_units_plus_prev_units_avg_2_prior,
    running_next_next_units,
    last_known_next_next_units
from enriched_store_sales
order by next_revenue;
