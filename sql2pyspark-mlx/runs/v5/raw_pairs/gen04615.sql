with cleaned as (
    select *, round(revenue / units, 2) revenue_per_unit
    from store_sales
    where month(sale_date) in (5, 7, 9)
),
scored as (
    select
        *,
        row_number() over (order by units asc, id asc) row_seq,
        ntile(3) over (order by revenue desc, id asc) revenue_bucket,
        rank() over (partition by product_id, store_id order by units desc nulls last) units_rank
    from cleaned
),
matched_keys as (
    select store_id, id
    from scored
    intersect all
    select store_id, id
    from store_sales
    where sale_date < '2025-04-01'
),
reconciled as (
    select s.*
    from scored s
    inner join matched_keys k on s.store_id = k.store_id and s.id = k.id
),
base as (
    select *, cast(revenue as int) revenue_int
    from reconciled
    where revenue_bucket between 4 and 6
),
scored_store_sales as (
    select
        store_id,
        revenue_bucket,
        units_rank,
        revenue_int,
        percent_rank() over (partition by product_id, store_id order by revenue_bucket asc nulls last) revenue_bucket_percent_rank,
        dense_rank() over (order by revenue_int asc) revenue_int_dense_rank
    from base
    where revenue >= 1600
)
select
    store_id,
    revenue_bucket,
    units_rank,
    revenue_int,
    revenue_int_dense_rank,
    round(revenue_bucket / sum(revenue_bucket) over (partition by store_id), 4) revenue_bucket_share
from scored_store_sales
