with attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    inner join promotions pr
        on e.channel = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on < pr.ends_on + interval 30 days
),
metrics as (
    -- Ranking and gap features
    select
        *,
        round(promo_id / sum(promo_id) over (partition by channel, promotion_promo_code), 4) as promo_id_share,
        avg(discount_pct) over (partition by promo_code order by ends_on asc, id asc, promo_id asc rows between 3 preceding and 1 preceding) as discount_pct_avg_3_prior,
        lag(discount_pct, 2) over (partition by channel order by discount_pct asc, id asc, promo_id asc) as prev_discount_pct
    from attributed
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by promotion_promo_code order by prev_discount_pct desc, id asc, promo_id asc) as event_seq,
        sum(promotion_discount_pct) over (partition by promo_code order by promo_id_share asc, id asc, promo_id asc rows between unbounded preceding and current row) as running_promotion_discount_pct,
        sum(prev_discount_pct) over (partition by channel order by discount_pct_avg_3_prior asc, id asc, promo_id asc rows between unbounded preceding and current row) as running_prev_discount_pct
    from metrics
)
-- Aggregate with conditional metrics
select
    case when running_promotion_discount_pct >= 2 then 'LARGE' else 'SMALL' end as running_promotion_discount_pct_tier,
    case when prev_discount_pct >= 0.36 then 'PREMIUM' else 'STANDARD' end as prev_discount_pct_tier,
    date_trunc('week', starts_on) as week_start,
    avg(event_seq) filter (where promotion_promo_code = 'BUNDLE') as bundle_avg_event_seq,
    count_if(promotion_promo_code = 'BUNDLE') as bundle_events,
    approx_percentile(promo_id, array(0.25, 0.5, 0.75)) as promo_id_quartiles,
    percent_rank() over (partition by case when running_promotion_discount_pct >= 2 then 'LARGE' else 'SMALL' end order by avg(event_seq) filter (where promotion_promo_code = 'BUNDLE') desc) as bundle_avg_event_seq_percent_rank
from enriched
group by case when running_promotion_discount_pct >= 2 then 'LARGE' else 'SMALL' end, case when prev_discount_pct >= 0.36 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', starts_on)
having bundle_events > 1
order by bundle_avg_event_seq asc
