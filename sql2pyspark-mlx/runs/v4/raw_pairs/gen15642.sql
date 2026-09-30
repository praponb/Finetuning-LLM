with valid as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        round(rating * 0.9, 2) as rating_scaled
    from suppliers
    where city is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(rating_scaled) over w_grp as country_max_rating_scaled,
        stddev_samp(rating) over w_grp_2 as city_stddev_samp_rating,
        first_value(rating_scaled) ignore nulls over (partition by city, country order by rating_scaled, id rows between unbounded preceding and current row) as first_known_rating_scaled
    from valid
    window w_grp as (partition by country), w_grp_2 as (partition by city)
),
matched_keys as (
    -- Set intersection on keys
    select country, id
    from scored
    intersect
    select country, id
    from suppliers
    where country != 'TH'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join matched_keys k on s.country = k.country and s.id = k.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.launch_date >= '2024-10-01'
        )
),
scored_suppliers as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_scaled,
        country_max_rating_scaled,
        city_stddev_samp_rating,
        first_known_rating_scaled,
        sum(rating_scaled) over (partition by city, country) as city_sum_rating_scaled,
        cume_dist() over (partition by country order by rating_scaled) as rating_scaled_cume_dist,
        rank() over (partition by country order by city_stddev_samp_rating desc) as city_stddev_samp_rating_rank,
        percent_rank() over (partition by city order by country_max_rating_scaled) as country_max_rating_scaled_percent_rank
    from eligible
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored_suppliers s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and size(c.tags) >= 2
        )
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by first_known_rating_scaled desc nulls last) as first_known_rating_scaled_rank,
        min(city_stddev_samp_rating_rank) over (partition by country) as country_min_city_stddev_samp_rating_rank,
        max(rating_scaled_cume_dist) over (partition by city) as city_max_rating_scaled_cume_dist
    from not_flagged
)
select
    id,
    supplier_name,
    city,
    country_max_rating_scaled,
    city_stddev_samp_rating,
    first_known_rating_scaled,
    rating_scaled_cume_dist,
    city_stddev_samp_rating_rank,
    country_max_rating_scaled_percent_rank
from enriched
order by city_stddev_samp_rating_rank;
