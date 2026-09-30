with matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on e.channel <=> pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on <= date_add(pr.ends_on, 7)
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_id,
        promotion_promo_code
    from matched
    qualify row_number() over (partition by promo_code, promotion_promo_code order by promotion_discount_pct desc nulls last, id asc, promo_id asc) = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by channel order by ends_on desc) as latest_rank
    from deduped
    qualify latest_rank = 1
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(latest_rank * 100, 2) as latest_rank_adjusted,
        coalesce(channel, 'N/A') as channel_clean
    from latest
    where channel is not null
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        channel,
        starts_on,
        promo_id,
        promotion_promo_code,
        latest_rank_adjusted,
        round(promo_id / sum(promo_id) over (partition by channel), 4) as promo_id_share,
        ntile(10) over (order by promo_id desc, id asc, promo_id asc) as promo_id_bucket,
        last_value(promo_id, true) over (partition by promotion_promo_code order by latest_rank asc, id asc, promo_id asc rows between unbounded preceding and current row) as last_known_promo_id,
        sum(latest_rank_adjusted) over w_running as running_latest_rank_adjusted
    from valid
    window w_running as (partition by promotion_promo_code, channel order by ends_on asc, id asc, promo_id asc rows between unbounded preceding and current row)
),
enriched_promotions as (
    -- Window metrics over several different partitions
    select
        *,
        sum(last_known_promo_id) over (order by promo_id_share asc, id asc, promo_id asc rows between unbounded preceding and current row) as running_last_known_promo_id,
        first_value(running_latest_rank_adjusted) ignore nulls over (partition by channel order by starts_on asc, id asc, promo_id asc rows between unbounded preceding and current row) as first_known_running_latest_rank_adjusted
    from enriched
),
metrics as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_id,
        promotion_promo_code,
        latest_rank_adjusted,
        promo_id_bucket,
        last_known_promo_id,
        running_last_known_promo_id,
        first_known_running_latest_rank_adjusted,
        cume_dist() over (partition by promotion_promo_code order by first_known_running_latest_rank_adjusted desc nulls last) as first_known_running_latest_rank_adjusted_cume_dist,
        lag(promo_id_bucket, 2) over (partition by promotion_promo_code order by latest_rank_adjusted asc, id asc, promo_id asc) as prev_promo_id_bucket,
        lead(running_last_known_promo_id, 1) over (partition by promotion_promo_code order by starts_on asc, id asc, promo_id asc) as next_running_last_known_promo_id
    from enriched_promotions
)
-- Multi-level aggregation
select
    case when promo_id_bucket >= 7 then 'LARGE' else 'SMALL' end as promo_id_bucket_tier,
    next_running_last_known_promo_id,
    count(*) as row_count,
    round(sum(running_last_known_promo_id) / nullif(count(case when channel = 'web' then 1 end), 0), 2) as running_last_known_promo_id_per_web,
    grouping_id() as grouping_id
from metrics
group by cube (case when promo_id_bucket >= 7 then 'LARGE' else 'SMALL' end, next_running_last_known_promo_id)
having row_count > 1
order by promo_id_bucket_tier desc;
