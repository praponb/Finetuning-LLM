with recent_stores as (
    select *
    from stores
    where store_name like 'Ai%'
),
stores_stats as (
    select region, max(sqft) as max_sqft, round((100 * sum(case when region = 'East' then 1 else 0 end)) / count(*), 1) as pct_east
    from recent_stores
    group by region
)
select region, max_sqft, pct_east
from stores_stats
where pct_east < 58
order by max_sqft desc, region
limit 20
