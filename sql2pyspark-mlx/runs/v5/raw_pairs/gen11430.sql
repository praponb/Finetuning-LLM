with kept_keys as (
    -- Set intersection on keys
    select product_id
    from store_sales
    intersect all
    select product_id
    from store_sales
    where revenue between 1100 and 2100
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from store_sales s
    inner join kept_keys k on s.product_id = k.product_id
),
cleaned as (
    -- Basic cleansing and defaults
    select *, last_day(sale_date) as sale_date_month_end
    from reconciled
    where sale_date > date '2023-09-18'
      and month(sale_date) in (4, 8, 9)
),
joined as (
    -- Attach products attributes
    select
        e.id,
        e.product_id,
        e.sale_date,
        e.revenue,
        b.cost
    from cleaned e
    inner join products b
        on e.product_id = b.id
    where b.stock_qty <> 207
),
scored as (
    -- Ranking and gap features
    select
        sale_date,
        cost,
        dense_rank() over (partition by product_id order by revenue desc nulls first) as revenue_dense_rank,
        percent_rank() over (partition by product_id order by revenue desc nulls last) as revenue_percent_rank
    from joined
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (order by revenue_percent_rank desc nulls last) as revenue_percent_rank_cume_dist,
        dense_rank() over (order by cost desc nulls last) as cost_dense_rank,
        dense_rank() over (order by revenue_dense_rank asc nulls last) as revenue_dense_rank_dense_rank
    from scored
)
-- Aggregate with conditional metrics
select
    date_trunc('year', sale_date) as year_start,
    case when revenue_dense_rank_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end as revenue_dense_rank_dense_rank_tier,
    count(*) as cnt,
    count(*) as row_count
from with_windows
group by cube (date_trunc('year', sale_date), case when revenue_dense_rank_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end)
having count(*) > 1
order by revenue_dense_rank_dense_rank_tier desc;
