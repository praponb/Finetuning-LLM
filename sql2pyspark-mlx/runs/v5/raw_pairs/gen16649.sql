with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as attr_key, attr_value
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        attr_key,
        explode_outer(coupon_codes) as code
    from flattened
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        b.email,
        b.credit_limit
    from exploded src
    left join customers b
        on src.customer_id = b.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        attr_key,
        code,
        email,
        credit_limit,
        coalesce(credit_limit, 0) as credit_limit_filled,
        upper(email) as email_upper
    from joined
    where email is null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(credit_limit) over (partition by attr_key) as attr_key_stddev_samp_credit_limit,
        rank() over (partition by code order by credit_limit_filled nulls last) as credit_limit_filled_rank,
        sum(credit_limit_filled) over (partition by code, attr_key) as code_sum_credit_limit_filled
    from cleaned
    where code = 'BUNDLE'
),
metrics as (
    -- Running and moving aggregates
    select
        attr_key,
        code,
        email,
        credit_limit_filled,
        credit_limit_filled_rank,
        code_sum_credit_limit_filled,
        dense_rank() over (order by credit_limit_filled nulls first) as credit_limit_filled_dense_rank,
        count(credit_limit_filled) over (partition by code, attr_key) as code_count_credit_limit_filled,
        stddev_samp(credit_limit_filled_rank) over (partition by attr_key, code) as attr_key_stddev_samp_credit_limit_filled_rank
    from scored
),
valid as (
    -- Basic cleansing and defaults
    select *, credit_limit_filled + attr_key_stddev_samp_credit_limit_filled_rank as credit_limit_filled_plus_attr_key_stddev_samp_credit_limit_filled_rank
    from metrics
    where code_count_credit_limit_filled != 6
),
stats as (
    -- Grouped metrics
    select
        code,
        case when credit_limit_filled_dense_rank >= 9 then 'HIGH' else 'LOW' end as credit_limit_filled_dense_rank_tier,
        sort_array(collect_set(attr_key)) as attr_key_list,
        min(credit_limit_filled_rank) as min_credit_limit_filled_rank,
        sum(case when code = 'BUNDLE' then credit_limit_filled else 0 end) as bundle_credit_limit_filled,
        percentile_approx(credit_limit_filled, 0.95) as credit_limit_filled_p95,
        sum(case when code = 'WELCOME5' then code_sum_credit_limit_filled else 0 end) as welcome5_code_sum_credit_limit_filled
    from valid
    group by 1, 2
    having credit_limit_filled_p95 >= 0
)
select
    code,
    credit_limit_filled_dense_rank_tier,
    attr_key_list,
    credit_limit_filled_p95
from stats;
