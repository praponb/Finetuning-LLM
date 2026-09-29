with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        ntile(4) over (partition by store_id order by revenue, id) as revenue_bucket,
        rank() over (partition by store_id order by units nulls last) as units_rank,
        stddev_samp(revenue) over (partition by store_id, product_id) as store_id_stddev_samp_revenue
    from store_sales
    where revenue between 1600 and 2500
),
scored as (
    -- Ranking and gap features
    select
        *,
        lag(units) over (partition by product_id order by units, id) as prev_units,
        lead(units) over (partition by product_id order by units, id) as next_units,
        lead(revenue) over (partition by product_id order by units, id) as next_revenue,
        row_number() over (partition by product_id order by units, id) as seq_no,
        last_value(store_id_stddev_samp_revenue) ignore nulls over (partition by product_id order by units_rank, id rows between unbounded preceding and current row) as last_known_store_id_stddev_samp_revenue
    from metrics
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by store_id order by units nulls last, id) as row_num
        from scored
    ) ranked
    where row_num = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        prev_units,
        last_known_store_id_stddev_samp_revenue,
        rank() over (order by next_units desc nulls last) as next_units_rank,
        avg(revenue) over (partition by product_id order by prev_units, id rows between 3 preceding and current row) as revenue_avg_3
    from deduped
)
-- Aggregate with conditional metrics
select
    store_id,
    round(avg(revenue_avg_3), 2) as avg_revenue_avg_3,
    percentile_approx(next_units_rank, 0.95) as next_units_rank_p95,
    sum(revenue_avg_3) as total_revenue_avg_3
from with_windows
group by store_id
having next_units_rank_p95 > 0
