with valid as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        rating,
        case when country in ('US', 'SG') then 'Tier 1' else 'Other' end as country_group,
        coalesce(city, 'DIRECT') as city_clean
    from suppliers
    where city is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(rating) over w_hist as last4_sum_rating,
        count(rating) over w_hist as last4_count_rating,
        max(rating) over w_hist as last4_max_rating
    from valid
    window w_hist as (partition by country order by rating, id rows between 3 preceding and current row)
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category in ('Sports', 'Home', 'Books')
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over (order by last4_max_rating, id) as last4_max_rating_bucket,
        avg(last4_max_rating) over (partition by country order by last4_count_rating, id rows between 2 preceding and 1 preceding) as last4_max_rating_avg_2_prior,
        avg(last4_max_rating) over (partition by country order by rating, id rows between 5 preceding and 1 preceding) as last4_max_rating_avg_5_prior
    from not_flagged
)
select
    id,
    country,
    rating,
    country_group,
    city_clean,
    last4_sum_rating,
    last4_count_rating,
    last4_max_rating_bucket,
    last4_max_rating_avg_2_prior,
    case when country rlike '^(JP|TH)$' then 'EXTENDED' when country rlike '^(US|SG)$' then 'GROUP_A' else 'CORE' end as country_zone
from with_windows
order by last4_sum_rating desc;
