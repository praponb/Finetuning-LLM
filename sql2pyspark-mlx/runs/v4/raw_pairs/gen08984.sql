with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        city,
        rating,
        cast(rating as int) as rating_int,
        regexp_replace(supplier_name, ' ', '-') as supplier_name_slug
    from suppliers
    where city is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(rating_int, 1) over (order by rating, id) as next_rating_int,
        ntile(10) over (partition by city order by rating_int desc, id) as rating_int_bucket,
        ntile(10) over (partition by city order by rating, id) as rating_bucket
    from cleaned
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and upper(trim(ch.product_name)) = 'OFFICE CHAIR'
        )
)
-- Multi-level aggregation
select
    case when rating_int >= 86 then 'HIGH' else 'LOW' end as rating_int_tier,
    city,
    sum(rating_bucket) as total_rating_bucket,
    sort_array(collect_set(city)) as city_list,
    percentile_approx(rating_int_bucket, array(0.25, 0.5, 0.75)) as rating_int_bucket_quartiles,
    round(sum(rating) / nullif(count(case when city = 'Tokyo' then 1 end), 0), 2) as rating_per_tokyo,
    count(*) filter (where city = 'Singapore') as singapore_rows,
    sort_array(collect_set(city)) as city_list_2
from kept
group by case when rating_int >= 86 then 'HIGH' else 'LOW' end, city
having rating_per_tokyo >= 0;
