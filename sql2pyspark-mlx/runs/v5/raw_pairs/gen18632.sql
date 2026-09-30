with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by revenue, id) as row_num
        from store_sales
    ) t
    where row_num = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        revenue,
        row_num,
        row_num - units as row_num_minus_units
    from latest
    where sale_date >= to_date('2023-07-31')
),
cleaned_store_sales as (
    -- Filter the raw rows and derive helper columns
    select *, row_num_minus_units - row_num as row_num_minus_units_minus_row_num
    from cleaned
    where store_id in (1, 2, 5, 6)
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        row_num_minus_units,
        min(revenue) over (partition by store_id order by revenue, id rows between unbounded preceding and current row) as running_min_revenue,
        avg(revenue) over (partition by store_id order by revenue, id rows between unbounded preceding and current row) as running_avg_revenue,
        sum(revenue) over (partition by store_id order by revenue, id rows between unbounded preceding and current row) as running_sum_revenue,
        count(revenue) over (partition by store_id order by revenue, id rows between unbounded preceding and current row) as running_count_revenue,
        ntile(5) over (partition by store_id order by revenue, id) as revenue_bucket
    from cleaned_store_sales
),
enriched_store_sales as (
    -- Window metrics over several different partitions
    select
        id,
        running_sum_revenue,
        running_count_revenue,
        revenue_bucket,
        lead(row_num_minus_units) over (partition by store_id order by running_sum_revenue, id) as next_row_num_minus_units,
        lag(row_num_minus_units) over (partition by store_id order by running_sum_revenue, id) as prev_row_num_minus_units,
        lead(running_min_revenue) over (partition by store_id order by running_sum_revenue, id) as next_running_min_revenue,
        lag(running_min_revenue) over (partition by store_id order by running_sum_revenue, id) as prev_running_min_revenue,
        row_number() over (partition by store_id order by running_sum_revenue, id) as event_seq
    from enriched
)
-- Grouped metrics
select
    case when running_sum_revenue >= 16500 then 'LARGE' else 'SMALL' end as running_sum_revenue_tier,
    case when next_running_min_revenue >= 3900 then 'LARGE' else 'SMALL' end as next_running_min_revenue_tier,
    round(avg(prev_running_min_revenue), 2) as avg_prev_running_min_revenue,
    min(prev_running_min_revenue) as min_prev_running_min_revenue,
    round(avg(running_count_revenue), 2) as avg_running_count_revenue,
    count(*) as event_count
from enriched_store_sales
group by 1, 2
having min_prev_running_min_revenue >= 0
