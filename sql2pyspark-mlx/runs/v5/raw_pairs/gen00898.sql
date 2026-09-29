with valid as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        initcap(trim(channel)) as channel_title,
        lower(channel) as channel_lower
    from promotions
    where channel is not null
      and channel in ('web', 'store', 'mobile')
),
enriched as (
    select
        id,
        promo_code,
        discount_pct,
        channel_lower,
        ntile(10) over (partition by promo_code order by discount_pct desc, id) as discount_pct_bucket,
        row_number() over w_ordered as row_seq,
        cume_dist() over w_recent as discount_pct_cume_dist,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share
    from valid
    where starts_on <= '2024-10-02'
    window w_ordered as (partition by promo_code order by ends_on desc, id), w_recent as (partition by promo_code order by discount_pct desc)
),
kept_keys as (
    select promo_code, id
    from enriched
    intersect
    select promo_code, id
    from promotions
    where promo_code = 'WELCOME5'
),
kept_rows as (
    select s.*
    from enriched s
    inner join kept_keys k on s.promo_code = k.promo_code and s.id = k.id
),
metrics as (
    select
        *,
        round(discount_pct_bucket / sum(discount_pct_bucket) over (partition by promo_code), 4) as discount_pct_bucket_share,
        round(discount_pct_bucket / sum(discount_pct_bucket) over (partition by promo_code), 4) as discount_pct_bucket_share_2,
        rank() over (partition by promo_code order by discount_pct_bucket nulls last) as discount_pct_bucket_rank,
        dense_rank() over (partition by promo_code order by row_seq) as row_seq_dense_rank
    from kept_rows
),
metrics_promotions as (
    select
        id,
        promo_code,
        discount_pct_bucket,
        row_seq,
        discount_pct_cume_dist,
        discount_pct_share,
        discount_pct_bucket_share,
        discount_pct_bucket_rank,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share_2
    from metrics
),
base as (
    select
        *,
        round(row_seq * 0.85, 2) as row_seq_adjusted,
        discount_pct_cume_dist + discount_pct_bucket as discount_pct_cume_dist_plus_discount_pct_bucket
    from metrics_promotions
    where discount_pct_bucket_share <= 0.42
),
filtered as (
    select
        id,
        promo_code,
        discount_pct_bucket,
        row_seq,
        discount_pct_cume_dist,
        discount_pct_share,
        discount_pct_bucket_share,
        row_seq_adjusted,
        discount_pct_cume_dist_plus_discount_pct_bucket,
        round(discount_pct_bucket_rank, 0) as discount_pct_bucket_rank_rounded,
        round(discount_pct_bucket_share * 100, 2) as discount_pct_bucket_share_adjusted
    from base
    where row_seq_adjusted < 79
),
totals as (
    select
        promo_code,
        min(discount_pct_share) as min_discount_pct_share,
        size(collect_set(promo_code)) as num_promo_codes,
        count(distinct id) as distinct_ids,
        count(case when promo_code = 'WELCOME5' then 1 end) as welcome5_count,
        size(collect_set(promo_code)) as num_promo_codes_2
    from filtered
    group by promo_code
)
select
    promo_code,
    min_discount_pct_share,
    num_promo_codes,
    distinct_ids,
    welcome5_count,
    num_promo_codes_2,
    case when promo_code rlike '^(WELCOME5|BUNDLE)$' then 'EXTENDED' else 'OTHER_ZONE' end as promo_code_zone
from totals
order by welcome5_count nulls first
