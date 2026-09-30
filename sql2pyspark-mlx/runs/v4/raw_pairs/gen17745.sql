with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as val
    from price_tiers
),
enriched as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        val,
        rank() over (partition by currency, plan order by val desc nulls first) as val_rank,
        dense_rank() over (partition by currency order by val nulls last) as val_dense_rank,
        cume_dist() over (order by val nulls last) as val_cume_dist,
        cume_dist() over (partition by plan, currency order by val nulls first) as val_cume_dist_2,
        count(val) over (partition by currency) as currency_count_val
    from flattened
),
matched_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect
    select id
    from price_tiers
    where effective_from > '2025-01-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('/', plan, currency) as plan_currency_key,
        val_dense_rank - val as val_dense_rank_minus_val
    from verified
    where plan = 'enterprise'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        min(val) over (partition by plan) as plan_min_val,
        rank() over (partition by currency, plan order by val_cume_dist_2 desc nulls last) as val_cume_dist_2_rank
    from base
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, val_cume_dist_2 + val_cume_dist as val_cume_dist_2_plus_val_cume_dist
    from metrics
    where currency_count_val = 14
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        val,
        val_rank,
        val_dense_rank,
        val_cume_dist,
        val_cume_dist_2,
        currency_count_val,
        plan_currency_key,
        val_dense_rank_minus_val,
        plan_min_val,
        val_cume_dist_2_rank,
        val_cume_dist_2_plus_val_cume_dist,
        max(val_cume_dist_2_plus_val_cume_dist) over w_grp as currency_max_val_cume_dist_2_plus_val_cume_dist,
        min(val_dense_rank_minus_val) over w_grp as currency_min_val_dense_rank_minus_val,
        max(val_cume_dist_2_plus_val_cume_dist) over w_running as plan_max_val_cume_dist_2_plus_val_cume_dist
    from cleaned
    window w_grp as (partition by currency), w_running as (partition by plan, currency)
)
select
    id,
    plan,
    effective_from,
    currency,
    val,
    val_rank,
    val_dense_rank,
    val_cume_dist,
    currency_count_val,
    case when currency rlike '^(EUR|THB)$' then 'GROUP_A' else 'CORE' end as currency_zone
from with_windows
order by currency desc nulls first, val_cume_dist desc
