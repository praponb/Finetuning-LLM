with base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        city,
        rating,
        concat_ws('-', country, supplier_name) as country_supplier_name_key
    from suppliers
    where lower(supplier_name) like '%aci%'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(rating / sum(rating) over (partition by city), 4) as rating_share,
        row_number() over w_grp as row_seq,
        first_value(city) ignore nulls over (partition by city order by rating, id rows between unbounded preceding and current row) as first_known_city
    from base
    window w_grp as (partition by city order by rating, id)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        city,
        rating,
        country_supplier_name_key,
        rating_share,
        row_seq,
        first_known_city,
        cume_dist() over (partition by city order by rating_share nulls first) as rating_share_cume_dist,
        cume_dist() over (partition by city order by row_seq nulls first) as row_seq_cume_dist
    from enriched
)
-- Grouped metrics
select
    city,
    count(*) filter (where city = 'Chiang Mai') as chiang_mai_rows,
    count(*) as num_rows,
    count(distinct id) as distinct_ids,
    round(avg(row_seq), 2) as avg_row_seq,
    percentile_approx(row_seq_cume_dist, 0.95) as row_seq_cume_dist_p95
from with_windows
group by 1
having num_rows > 1
