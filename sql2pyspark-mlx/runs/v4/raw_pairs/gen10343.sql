with with_windows as (
    -- Ranking and gap features
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        avg(rating) over (order by rating, id rows between 3 preceding and current row) as rating_avg_3,
        row_number() over (partition by city order by rating, id) as seq_no
    from suppliers
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by country, city order by rating desc nulls last) as rating_percent_rank,
        sum(rating_avg_3) over (order by seq_no, id rows between unbounded preceding and current row) as running_rating_avg_3
    from with_windows
),
matched_keys as (
    -- Set intersection on keys
    select country, id
    from enriched
    intersect all
    select country, id
    from suppliers
    where city = 'Berlin'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.country = k.country and s.id = k.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, least(rating_avg_3, running_rating_avg_3) as least_rating_avg_3_running_rating_avg_3
    from reconciled
    where city is not null
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty = 300
        )
)
-- Ranking and gap features
select
    *,
    cume_dist() over w_running as rating_avg_3_cume_dist,
    rank() over (partition by city, country order by rating nulls first) as rating_rank
from eligible
window w_running as (partition by city order by rating_avg_3 nulls first)
