with valid as (
    select
        *,
        length(supplier_name) as supplier_name_length,
        case when city in ('London', 'Bangkok') then 'Priority' else 'Other' end as city_group
    from suppliers
    where rating between 2.4 and 3.2
),
filtered as (
    select
        id,
        supplier_name,
        city,
        rating,
        supplier_name_length,
        city_group,
        upper(country) as country_upper,
        coalesce(city, 'DIRECT') as city_filled
    from valid
    where city is not null
),
enriched as (
    select
        *,
        avg(supplier_name_length) over w_hist as supplier_name_length_avg_5_prior,
        avg(supplier_name_length) over (partition by city order by supplier_name_length, id rows between 3 preceding and 1 preceding) as supplier_name_length_avg_3_prior,
        row_number() over (order by rating, id) as row_seq,
        round(rating / sum(rating) over (partition by city), 4) as rating_share,
        last_value(rating) ignore nulls over (partition by city order by supplier_name_length, id rows between unbounded preceding and current row) as last_known_rating
    from filtered
    window w_hist as (partition by city order by supplier_name_length, id rows between 5 preceding and 1 preceding)
),
daily as (
    select
        city,
        percentile_approx(rating_share, 0.5) as rating_share_p50,
        count(*) as cnt,
        stddev_samp(rating) as rating_stddev,
        approx_percentile(supplier_name_length_avg_5_prior, 0.5) as supplier_name_length_avg_5_prior_p50,
        grouping_id() as grouping_level
    from enriched
    group by cube (city)
    having stddev_samp(rating) >= 0
)
select
    city,
    rating_share_p50,
    cnt,
    rating_stddev,
    supplier_name_length_avg_5_prior_p50,
    grouping_level
from daily
