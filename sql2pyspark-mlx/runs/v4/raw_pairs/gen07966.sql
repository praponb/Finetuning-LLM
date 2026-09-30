with ranked as (
    -- QUALIFY-style window filter
    select *
    from promotions
    qualify row_number() over (partition by promo_code order by starts_on desc, id asc) = 1
),
ranked_promotions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by promo_code order by ends_on desc, id asc) as rn
    from ranked
    qualify rn = 1
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        ceil(rn) as rn_whole,
        coalesce(channel, 'N/A') as resolved_channel
    from ranked_promotions
    where channel is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by channel order by rn desc, id asc) as row_seq,
        lead(rn_whole, 1) over (partition by promo_code order by starts_on asc, id asc) as next_rn_whole,
        sum(discount_pct) over (partition by promo_code order by rn_whole asc, id asc rows between unbounded preceding and current row) as running_discount_pct
    from base
),
matched_keys as (
    -- Set difference on keys
    select channel, promo_code
    from scored
    except all
    select channel, promo_code
    from promotions
    where starts_on >= '2024-03-10'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join matched_keys k on s.channel = k.channel and s.promo_code = k.promo_code
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from reconciled e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.starts_on between pr.starts_on and pr.ends_on
),
summary as (
    -- Multi-level aggregation
    select
        case when rn_whole >= 67 then 'PREMIUM' else 'STANDARD' end as rn_whole_tier,
        case when next_rn_whole >= 79 then 'PREMIUM' else 'STANDARD' end as next_rn_whole_tier,
        case when row_seq >= 9 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
        count(*) filter (where channel = 'mobile') as mobile_rows,
        count(*) filter (where promotion_promo_code = 'SPRING10') as spring10_rows,
        sum(promotion_discount_pct) as total_promotion_discount_pct,
        sum(promotion_discount_pct) filter (where promo_code = 'BUNDLE') as bundle_total_promotion_discount_pct
    from attributed
    group by cube (case when rn_whole >= 67 then 'PREMIUM' else 'STANDARD' end, case when next_rn_whole >= 79 then 'PREMIUM' else 'STANDARD' end, case when row_seq >= 9 then 'PREMIUM' else 'STANDARD' end)
)
select
    rn_whole_tier,
    next_rn_whole_tier,
    row_seq_tier,
    mobile_rows,
    spring10_rows,
    total_promotion_discount_pct,
    bundle_total_promotion_discount_pct
from summary
order by spring10_rows asc nulls first;
