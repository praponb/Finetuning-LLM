with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) li as key, value
),
cleaned as (
    -- Basic cleansing and defaults
    select *, array_join(coupon_codes, '|') as coupon_codes_text
    from lines
    where key is not null
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        d.age,
        d.email,
        d.segment
    from cleaned a
    left join customers d
        on a.customer_id <=> d.id
    where d.signup_date >= '2023-05-01'
),
enriched as (
    -- Window metrics over several different partitions
    select *, dense_rank() over (order by age) as age_dense_rank
    from joined
),
enriched_carts as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        key,
        value,
        email,
        age_dense_rank,
        dense_rank() over (partition by customer_id, segment order by age_dense_rank nulls first) as age_dense_rank_dense_rank,
        max(age) over (partition by key) as key_max_age,
        dense_rank() over (order by age desc) as age_dense_rank_2,
        round(age / sum(age) over (partition by segment, customer_id), 4) as age_share
    from enriched
),
stats as (
    -- Multi-level aggregation
    select
        case when age_share >= 0.89 then 'PREMIUM' else 'STANDARD' end as age_share_tier,
        case when age_dense_rank_2 >= 6 then 'LARGE' else 'SMALL' end as age_dense_rank_2_tier,
        size(collect_set(key)) as num_keys,
        count_if(key = 'device') as device_events,
        max(age_dense_rank_2) as max_age_dense_rank_2
    from enriched_carts
    group by age_share_tier, age_dense_rank_2_tier
    having num_keys >= 1
)
select
    age_share_tier,
    age_dense_rank_2_tier,
    device_events,
    max_age_dense_rank_2
from stats
order by max_age_dense_rank_2 desc nulls first, age_dense_rank_2_tier
