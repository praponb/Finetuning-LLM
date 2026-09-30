with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock between '2024-11-01' and '2024-12-01'
        )
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(store_name) as clean_store_name,
        coalesce(region, 'DIRECT') as region_filled
    from kept
    where year(opened_date) >= 2023
),
kept_stores as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date < s.opened_date
        )
)
-- Running and moving aggregates
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    clean_store_name,
    region_filled,
    ntile(3) over w_part as sqft_bucket,
    sum(sqft) over (order by opened_date, id rows between unbounded preceding and current row) as cumulative_sqft,
    avg(sqft) over w_recent as sqft_avg_2,
    avg(sqft) over (partition by region, city order by sqft, id rows between 5 preceding and 1 preceding) as sqft_avg_5_prior
from kept_stores
window w_part as (partition by city order by sqft, id), w_recent as (partition by city order by sqft, id rows between 2 preceding and current row)
