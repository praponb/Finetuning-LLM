with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        datediff(current_date(), starts_on) as days_since_starts_on,
        upper(promo_code) as promo_code_upper,
        coalesce(channel, 'UNKNOWN') as channel_clean
    from promotions
    where starts_on > to_date('2023-09-27')
      and channel is not null
      and channel = 'web'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over w_hist as row_seq,
        dense_rank() over w_hist_2 as discount_pct_dense_rank,
        ntile(4) over (partition by channel, promo_code order by discount_pct desc, id) as discount_pct_bucket,
        first_value(channel) ignore nulls over (partition by channel order by starts_on, id rows between unbounded preceding and current row) as first_known_channel
    from base
    window w_hist as (partition by promo_code, channel order by discount_pct, id), w_hist_2 as (order by discount_pct desc)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        days_since_starts_on,
        promo_code_upper,
        channel_clean,
        row_seq,
        discount_pct_dense_rank,
        discount_pct_bucket,
        first_known_channel,
        row_number() over (order by ends_on, id) as row_seq_2,
        ntile(10) over (partition by channel, promo_code order by discount_pct_bucket desc, id) as discount_pct_bucket_bucket,
        dense_rank() over (order by days_since_starts_on desc nulls first) as days_since_starts_on_dense_rank,
        first_value(discount_pct_bucket) ignore nulls over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as first_known_discount_pct_bucket,
        last_value(days_since_starts_on) ignore nulls over (partition by promo_code, channel order by starts_on, id rows between unbounded preceding and current row) as last_known_days_since_starts_on
    from enriched
)
-- Native SQL PIVOT
select *
from (
    select
        channel,
        promo_code,
        days_since_starts_on_dense_rank
    from metrics
) src
pivot (
    sum(days_since_starts_on_dense_rank)
    for promo_code in ('BUNDLE' as bundle, 'WELCOME5' as welcome5)
)
