with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode(tier_prices) as tier_value
    from price_tiers
),
metrics as (
    -- Ranking and gap features
    select
        id,
        tier_value,
        max(tier_value) over (partition by currency) as currency_max_tier_value,
        max(tier_value) over (partition by currency) as currency_max_tier_value_2,
        stddev_samp(tier_value) over (partition by currency) as currency_stddev_samp_tier_value
    from flattened
    where tier_value >= 44
),
matched_keys as (
    -- Set difference on keys
    select id
    from metrics
    except distinct
    select id
    from price_tiers
    where plan = 'enterprise'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.id = k.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        tier_value,
        currency_max_tier_value,
        currency_max_tier_value_2,
        currency_stddev_samp_tier_value,
        currency_max_tier_value_2 + currency_max_tier_value as currency_max_tier_value_2_plus_currency_max_tier_value,
        cast(currency_max_tier_value as string) as currency_max_tier_value_num
    from reconciled
    where currency_max_tier_value_2 = 29
)
-- Aggregate with conditional metrics
select
    case when currency_max_tier_value_2_plus_currency_max_tier_value >= 80 then 'LARGE' else 'SMALL' end as currency_max_tier_value_2_plus_currency_max_tier_value_tier,
    count(distinct id) as distinct_ids,
    round(avg(currency_stddev_samp_tier_value), 2) as avg_currency_stddev_samp_tier_value,
    approx_percentile(currency_max_tier_value, array(0.25, 0.5, 0.75)) as currency_max_tier_value_quartiles,
    percentile_approx(currency_max_tier_value_2_plus_currency_max_tier_value, array(0.25, 0.5, 0.75)) as currency_max_tier_value_2_plus_currency_max_tier_value_quartiles,
    count(*) as num_rows,
    approx_percentile(currency_stddev_samp_tier_value, array(0.25, 0.5, 0.75)) as currency_stddev_samp_tier_value_quartiles
from base
group by currency_max_tier_value_2_plus_currency_max_tier_value_tier
having num_rows > 1;
