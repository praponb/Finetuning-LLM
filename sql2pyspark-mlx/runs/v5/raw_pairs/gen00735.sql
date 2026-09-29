with enriched as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        first_value(channel, true) over (partition by channel, promo_code order by starts_on, id rows between unbounded preceding and current row) as first_known_channel,
        avg(discount_pct) over w_hist as discount_pct_avg_5
    from promotions
    where channel != 'partner'
    window w_hist as (partition by promo_code order by ends_on, id rows between 5 preceding and current row)
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from enriched s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval 14 days
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        first_known_channel,
        promo_id,
        promotion_promo_code,
        count(promo_id) over w_ordered as last7_count_promo_id,
        max(promo_id) over w_ordered as last7_max_promo_id,
        avg(promo_id) over w_ordered as last7_avg_promo_id
    from with_window
    window w_ordered as (partition by channel, promo_code order by discount_pct, id, promo_id rows between 6 preceding and current row)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        last7_count_promo_id,
        last7_max_promo_id,
        last7_avg_promo_id,
        cume_dist() over (partition by promo_code order by discount_pct) as discount_pct_cume_dist,
        row_number() over w_ordered as row_seq,
        lead(last7_count_promo_id, 2) over (partition by promotion_promo_code order by last7_count_promo_id, id, promo_id) as next_last7_count_promo_id
    from scored
    window w_ordered as (partition by channel order by last7_count_promo_id desc, id, promo_id)
),
enriched_promotions as (
    -- Running and moving aggregates
    select
        id,
        promo_id,
        promotion_promo_code,
        last7_avg_promo_id,
        row_seq,
        avg(last7_avg_promo_id) over (partition by promo_code, promotion_promo_code order by ends_on, id, promo_id rows between 3 preceding and current row) as last4_avg_last7_avg_promo_id,
        min(last7_avg_promo_id) over (partition by promo_code, promotion_promo_code order by ends_on, id, promo_id rows between 3 preceding and current row) as last4_min_last7_avg_promo_id,
        count(last7_avg_promo_id) over (partition by promo_code, promotion_promo_code order by ends_on, id, promo_id rows between 3 preceding and current row) as last4_count_last7_avg_promo_id,
        max(last7_avg_promo_id) over (partition by promo_code, promotion_promo_code order by ends_on, id, promo_id rows between 3 preceding and current row) as last4_max_last7_avg_promo_id
    from metrics
)
-- Grouped metrics
select
    promo_id,
    sum(last4_avg_last7_avg_promo_id) filter (where promotion_promo_code = 'BUNDLE') as bundle_total_last4_avg_last7_avg_promo_id,
    count(distinct promotion_promo_code) as distinct_promotion_promo_codes,
    sum(case when promotion_promo_code = 'VIP' then last4_avg_last7_avg_promo_id else 0 end) as vip_last4_avg_last7_avg_promo_id,
    count(distinct promotion_promo_code) as distinct_promotion_promo_codes_2,
    percentile_approx(last7_avg_promo_id, array(0.25, 0.5, 0.75)) as last7_avg_promo_id_quartiles,
    round(avg(promo_id), 2) as avg_promo_id
from enriched_promotions
group by promo_id
having count(distinct promotion_promo_code) > 1
order by distinct_promotion_promo_codes_2 desc nulls first
