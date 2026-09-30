with metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by rating desc) as rating_cume_dist,
        round(rating / sum(rating) over (partition by city), 4) as rating_share
    from suppliers
),
metrics_suppliers as (
    -- Running and moving aggregates
    select
        *,
        lead(rating, 1) over (partition by city order by rating_share, id) as next_rating,
        avg(rating_cume_dist) over (partition by country order by rating_share, id rows between 2 preceding and current row) as rating_cume_dist_avg_2,
        sum(rating_cume_dist) over (order by rating, id rows between unbounded preceding and current row) as cumulative_rating_cume_dist,
        ntile(3) over (partition by city, country order by rating, id) as rating_bucket,
        max(rating_share) over (partition by city) as city_max_rating_share
    from metrics
),
scored as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over w_hist as rating_bucket_bucket,
        ntile(3) over (partition by country order by rating_bucket, id) as rating_bucket_bucket_2
    from metrics_suppliers
    window w_hist as (partition by country order by rating_bucket desc, id)
),
matched_keys as (
    -- Set difference on keys
    select id, city
    from scored
    except
    select id, city
    from suppliers
    where supplier_name like 'Tok%'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    join matched_keys k on s.id = k.id and s.city = k.city
),
scored_suppliers as (
    -- Running and moving aggregates
    select
        *,
        min(rating_share) over (partition by city, country) as city_min_rating_share,
        cume_dist() over (partition by country order by rating) as rating_cume_dist_2
    from kept_rows
    where city_max_rating_share != 0
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        rating_bucket_bucket_2 - city_max_rating_share as rating_bucket_bucket_2_minus_city_max_rating_share,
        coalesce(city, 'NONE') as city_filled
    from scored_suppliers
    where country = 'SG'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.product_name like '%Wate%'
        )
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        city,
        rating_cume_dist_2,
        rating_cume_dist,
        rating
    from eligible
) src
unpivot (
    value for measure in (rating_cume_dist_2 as RATING_CUME_DIST_2_VAL, rating_cume_dist as RATING_CUME_DIST_VAL, rating as RATING)
);
