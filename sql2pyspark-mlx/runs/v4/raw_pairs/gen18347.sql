with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by channel order by starts_on desc, id) as dedup_rank
        from promotions
    ) t
    where dedup_rank = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(discount_pct) over (partition by promo_code order by discount_pct, id rows between 3 preceding and 1 preceding) as discount_pct_avg_3_prior,
        ntile(10) over (partition by channel order by discount_pct, id) as discount_pct_bucket
    from latest
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(discount_pct) over (partition by promo_code, channel order by dedup_rank, id rows between 6 preceding and current row) as discount_pct_avg_6,
        row_number() over (partition by promo_code, channel order by starts_on desc, id) as row_seq,
        first_value(discount_pct_bucket) ignore nulls over (order by discount_pct_avg_3_prior, id rows between unbounded preceding and current row) as first_known_discount_pct_bucket,
        sum(discount_pct_avg_3_prior) over (partition by channel, promo_code order by ends_on, id rows between unbounded preceding and current row) as running_discount_pct_avg_3_prior,
        row_number() over (partition by channel order by discount_pct_avg_3_prior desc, id) as row_seq_2
    from scored
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        first_value(discount_pct_avg_6, true) over (order by discount_pct_avg_6, id rows between unbounded preceding and current row) as first_known_discount_pct_avg_6,
        ntile(5) over (order by running_discount_pct_avg_3_prior, id) as running_discount_pct_avg_3_prior_bucket,
        sum(discount_pct_avg_3_prior) over (partition by promo_code order by running_discount_pct_avg_3_prior, id rows between unbounded preceding and current row) as running_discount_pct_avg_3_prior_2
    from enriched
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows e
    left join promotions pr
        on e.channel = pr.channel
       and e.ends_on between pr.starts_on and pr.ends_on
),
with_windows_promotions as (
    -- Ranking and gap features
    select
        *,
        round(promotion_discount_pct / sum(promotion_discount_pct) over (partition by promotion_promo_code, promo_code), 4) as promotion_discount_pct_share,
        avg(discount_pct_bucket) over (order by dedup_rank, id, promo_id rows between 2 preceding and current row) as discount_pct_bucket_avg_2
    from attributed
),
scored_promotions as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by promotion_promo_code, promo_code order by row_seq_2, id, promo_id) as event_seq,
        lag(row_seq_2, 1) over (order by promotion_discount_pct, id, promo_id) as prev_row_seq_2,
        sum(discount_pct_avg_6) over (partition by channel order by first_known_discount_pct_bucket, id, promo_id rows between unbounded preceding and current row) as running_discount_pct_avg_6
    from with_windows_promotions
)
-- Grouped metrics
select
    case when row_seq >= 7 then 'HIGH' else 'LOW' end as row_seq_tier,
    date_trunc('quarter', starts_on) as quarter_start,
    sum(case when promotion_promo_code = 'BUNDLE' then first_known_discount_pct_avg_6 else 0 end) as bundle_first_known_discount_pct_avg_6,
    approx_percentile(prev_row_seq_2, 0.5) as prev_row_seq_2_p50,
    dense_rank() over (partition by case when row_seq >= 7 then 'HIGH' else 'LOW' end order by approx_percentile(prev_row_seq_2, 0.5) desc) as prev_row_seq_2_p50_dense_rank
from scored_promotions
group by case when row_seq >= 7 then 'HIGH' else 'LOW' end, date_trunc('quarter', starts_on)
having bundle_first_known_discount_pct_avg_6 >= 0
