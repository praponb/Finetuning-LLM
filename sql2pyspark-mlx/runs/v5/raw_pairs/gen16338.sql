with valid as (
    -- Basic cleansing and defaults
    select
        id,
        sale_date,
        revenue,
        case when revenue >= 3200 then 'High' when revenue >= 900 then 'Medium' else 'Low' end as revenue_level,
        date_add(sale_date, 78) as sale_date_plus_78d
    from store_sales
    where sale_date >= date '2023-05-13'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by revenue desc nulls last) as revenue_dense_rank,
        lead(revenue, 2) over (order by sale_date, id) as next_revenue,
        sum(revenue) over w_ordered as cumulative_revenue,
        row_number() over (order by sale_date_plus_78d desc, id) as seq_no,
        avg(revenue) over (order by revenue, id rows between 3 preceding and 1 preceding) as revenue_avg_3_prior
    from valid
    window w_ordered as (order by sale_date, id rows between unbounded preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (order by seq_no desc, id) as seq_no_bucket,
        ntile(10) over (order by cumulative_revenue, id) as cumulative_revenue_bucket
    from metrics
),
valid_store_sales as (
    -- Basic cleansing and defaults
    select
        id,
        sale_date,
        revenue,
        revenue_level,
        revenue_dense_rank,
        next_revenue,
        seq_no,
        revenue_avg_3_prior,
        seq_no_bucket,
        cumulative_revenue_bucket,
        coalesce(cumulative_revenue, 0) as cumulative_revenue_filled
    from enriched
    where seq_no_bucket = 5
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        sale_date,
        revenue,
        revenue_level,
        revenue_dense_rank,
        next_revenue,
        seq_no,
        revenue_avg_3_prior,
        seq_no_bucket,
        datediff(current_date(), sale_date) as days_since_sale_date,
        round(revenue_dense_rank, 2) as revenue_dense_rank_rounded
    from valid_store_sales
    where sale_date > date '2023-05-17'
),
kept_keys as (
    -- Set intersection on keys
    select id
    from filtered
    intersect
    select id
    from store_sales
    where product_id in (1, 4, 7, 9)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.id = k.id
),
valid_store_sales_2 as (
    -- Filter the raw rows and derive helper columns
    select *, date_trunc('year', sale_date) as sale_date_period
    from kept_rows
    where sale_date >= date '2023-05-23'
)
-- Window metrics over several different partitions
select
    id,
    sale_date,
    revenue,
    next_revenue,
    revenue_avg_3_prior,
    cume_dist() over (order by revenue desc) as revenue_cume_dist,
    percent_rank() over (order by revenue_avg_3_prior desc nulls last) as revenue_avg_3_prior_percent_rank
from valid_store_sales_2;
