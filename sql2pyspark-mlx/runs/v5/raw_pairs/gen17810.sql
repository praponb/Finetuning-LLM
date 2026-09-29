with not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from suppliers s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.supplier_id in (3, 29, 33)
        )
),
kept_keys as (
    -- Set intersection on keys
    select country, city
    from not_flagged
    intersect all
    select country, city
    from suppliers
    where city = 'Tokyo'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    join kept_keys k on s.country = k.country and s.city = k.city
),
scored as (
    -- Ranking and gap features
    select
        supplier_name,
        country,
        city,
        rating,
        rank() over (order by rating desc nulls first) rating_rank,
        percent_rank() over (partition by country, city order by rating asc nulls first) rating_percent_rank,
        stddev_samp(rating) over (partition by city) city_stddev_samp_rating,
        round(rating / sum(rating) over (partition by country, city), 4) rating_share,
        count(rating) over (partition by city) city_count_rating
    from verified
    where city is null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by city order by rating desc) rating_rank_2,
        cume_dist() over (order by rating_percent_rank desc nulls last) rating_percent_rank_cume_dist,
        max(rating) over (partition by city) city_max_rating,
        rank() over (partition by city order by rating asc nulls last) rating_rank_3
    from scored
)
-- Running and moving aggregates
select
    supplier_name,
    country,
    city,
    rating_rank,
    city_stddev_samp_rating,
    rating_share,
    city_count_rating,
    dense_rank() over (partition by city order by rating_percent_rank desc) rating_percent_rank_dense_rank,
    stddev_samp(rating) over (partition by city) city_stddev_samp_rating_2
from with_windows;
