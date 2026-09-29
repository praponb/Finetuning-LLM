with exploded as (
    -- Unnest the array into one row per element
    select id, explode(coupon_codes) as tag
    from carts
),
valid as (
    -- Keep recent, valid rows
    select *, trim(tag) as clean_tag
    from exploded
    where tag is not null
),
base as (
    -- Basic cleansing and defaults
    select *, length(tag) as tag_length
    from valid
    where tag in ('VIP', 'WELCOME5', 'BUNDLE')
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        tag,
        clean_tag,
        tag_length,
        dense_rank() over (order by tag_length asc) as tag_length_dense_rank
    from base
    where tag_length is not null
),
base_carts as (
    -- Basic cleansing and defaults
    select
        id,
        tag,
        clean_tag,
        tag_length_dense_rank,
        upper(tag) as tag_upper,
        coalesce(tag, 'UNKNOWN') as tag_filled
    from metrics
    where tag_length_dense_rank < 3
),
enriched as (
    -- Running and moving aggregates
    select *, sum(tag_length_dense_rank) over (partition by tag) as tag_sum_tag_length_dense_rank
    from base_carts
),
daily as (
    -- Multi-level aggregation
    select
        tag,
        count(*) as event_count,
        count(distinct id) as distinct_ids
    from enriched
    group by tag
)
select tag, distinct_ids
from daily
order by tag asc, distinct_ids asc;
