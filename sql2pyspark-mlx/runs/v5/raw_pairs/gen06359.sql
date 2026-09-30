with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        sqft,
        date_format(opened_date, 'yyyy-MM') as opened_date_label,
        concat_ws('-', region, city) as region_city_key,
        coalesce(region, 'NONE') as region_clean
    from stores
    where opened_date >= to_date('2023-06-26')
      and region is not null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.customer_id = 22
        )
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share,
        lead(sqft, 1) over (partition by city order by sqft, id) as next_sqft,
        lead(sqft, 1) over w_hist as next_sqft_2
    from not_flagged
    where store_name like 'Old%'
    window w_hist as (order by sqft, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.product_id in (2, 5, 7)
        )
),
enriched as (
    -- Ranking and gap features
    select
        *,
        first_value(next_sqft) ignore nulls over (partition by region, city order by sqft_share, id rows between unbounded preceding and current row) as first_known_next_sqft,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share_2
    from kept
),
grouped as (
    -- Grouped metrics
    select
        region,
        round(sum(first_known_next_sqft) / nullif(count(case when city = 'Bangkok' then 1 end), 0), 2) as first_known_next_sqft_per_bangkok,
        bit_and(sqft) as sqft_bit_and,
        percentile_approx(next_sqft, array(0.25, 0.5, 0.75)) as next_sqft_quartiles,
        sum(sqft_share) filter (where city = 'Chiang Mai') as chiang_mai_total_sqft_share,
        round(sum(sqft_share) / nullif(count(case when city = 'Bangkok' then 1 end), 0), 2) as sqft_share_per_bangkok
    from enriched
    group by cube (region)
)
select
    region,
    first_known_next_sqft_per_bangkok,
    sqft_bit_and,
    next_sqft_quartiles,
    case when region rlike '^(South|North)$' then 'PRIORITY' else 'OTHER_ZONE' end as region_zone
from grouped
order by sqft_bit_and desc, region
