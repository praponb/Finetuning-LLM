with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, revenue + units as revenue_per_unit
    from store_sales
    where sale_date >= date '2023-02-19'
),
filtered as (
    -- Basic cleansing and defaults
    select *, date_trunc('year', sale_date) as sale_date_period
    from cleaned
    where sale_date > date '2023-06-04'
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when sale_date_period - lag(sale_date_period) over (partition by product_id order by sale_date_period, id) > interval '60' day then 1 else 0 end as is_new_session
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        sale_date_period,
        sum(is_new_session) over (partition by product_id order by sale_date_period, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(sale_date_period) over (partition by store_id order by sale_date_period, id) is null
                 or sale_date_period > lag(sale_date_period) over (partition by store_id order by sale_date_period, id) + interval '14' day then 1 else 0 end as new_group_flag
    from islands
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        store_id,
        revenue,
        revenue_per_unit,
        sale_date_period,
        session_id,
        sum(new_group_flag) over (partition by store_id order by sale_date_period, id rows between unbounded preceding and current row) as session_no
    from flagged
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(revenue_per_unit) over (partition by store_id order by revenue, id) as prev_revenue_per_unit,
        lead(revenue_per_unit) over (partition by store_id order by revenue, id) as next_revenue_per_unit,
        lead(session_id) over (partition by store_id order by revenue, id) as next_session_id,
        row_number() over (partition by store_id order by revenue, id) as event_seq,
        cume_dist() over (partition by store_id order by session_no desc) as session_no_cume_dist
    from sessionized
    where sale_date_period >= '2024-10-01'
)
-- Window metrics over several different partitions
select
    id,
    revenue,
    session_id,
    session_no,
    next_session_id,
    event_seq,
    last_value(revenue_per_unit, true) over (partition by store_id order by session_no_cume_dist, id rows between unbounded preceding and current row) as last_known_revenue_per_unit,
    row_number() over (order by revenue_per_unit, id) as seq_no,
    lead(revenue_per_unit, 1, 0) over w_grp as next_revenue_per_unit_2
from scored
window w_grp as (partition by store_id order by revenue, id);
