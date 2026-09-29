with base as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        case when discount_pct >= 0.3 then 'High' when discount_pct >= 0.18 then 'Medium' else 'Low' end as discount_pct_tier,
        date_trunc('month', starts_on) as starts_on_period
    from promotions
    where ends_on >= date '2023-08-30'
      and promo_code in ('BUNDLE', 'SPRING10')
),
metrics as (
    -- Ranking and gap features
    select
        *,
        row_number() over w_recent as row_seq,
        max(discount_pct) over (partition by channel order by cast(starts_on_period as bigint) range between 604800 preceding and current row) as discount_pct_max_7d
    from base
    window w_recent as (order by discount_pct, id)
),
base_promotions as (
    -- Basic cleansing and defaults
    select *, datediff(current_date(), starts_on) as days_since_starts_on
    from metrics
    where channel is not null
      and to_date(starts_on_period) >= '2025-01-01'
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by channel order by row_seq desc, id) as latest_rank
    from base_promotions
    qualify latest_rank = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct
    from latest a
    inner join promotions pr
        on a.channel = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when starts_on_period - lag(starts_on_period) over (partition by channel order by starts_on_period, id, promo_id) > interval '30' day then 1 else 0 end as is_new_session
    from matched
),
sessions_promotions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by channel order by starts_on_period, id, promo_id rows between unbounded preceding and current row) as burst_id
    from flagged
)
-- Aggregate with conditional metrics
select
    channel,
    case when is_new_session >= 1 then 'PREMIUM' else 'STANDARD' end as is_new_session_tier,
    sum(case when channel = 'mobile' then promotion_discount_pct else 0 end) as mobile_promotion_discount_pct,
    sort_array(collect_set(promo_code)) as promo_code_list,
    avg(row_seq) as avg_row_seq,
    grouping_id() as grouping_level
from sessions_promotions
group by grouping sets ((channel, case when is_new_session >= 1 then 'PREMIUM' else 'STANDARD' end), (channel), ());
