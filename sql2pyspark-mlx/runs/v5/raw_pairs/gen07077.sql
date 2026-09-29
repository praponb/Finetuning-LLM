with metrics as (
    -- Ranking and gap features
    select
        *,
        lead(discount_pct) over (partition by channel order by ends_on, id) as next_discount_pct,
        row_number() over (partition by channel order by ends_on, id) as row_seq
    from promotions
    where discount_pct between 0.22 and 0.28
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from metrics s
    inner join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval 14 days
),
enriched as (
    -- Ranking and gap features
    select
        id,
        starts_on,
        discount_pct,
        next_discount_pct,
        row_seq,
        promo_id,
        promotion_discount_pct,
        min(row_seq) over (partition by channel) as channel_min_row_seq,
        cume_dist() over (partition by channel, promo_code order by row_seq) as row_seq_cume_dist
    from attributed
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, channel_min_row_seq - row_seq_cume_dist as channel_min_row_seq_minus_row_seq_cume_dist
    from enriched
    where row_seq = 6
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when next_discount_pct >= 0.28 then 'High' when next_discount_pct >= 0.12 then 'Medium' else 'Low' end as next_discount_pct_band
    from filtered
    where promo_id is not null
)
-- Multi-level aggregation
select
    date_trunc('month', starts_on) as activity_month,
    promo_id,
    percentile_approx(promo_id, 0.95) as promo_id_p95,
    approx_percentile(promotion_discount_pct, 0.9) as promotion_discount_pct_p90,
    round(avg(promo_id), 2) as avg_promo_id,
    percentile_approx(promotion_discount_pct, 0.5) as promotion_discount_pct_p50
from cleaned
group by date_trunc('month', starts_on), promo_id
order by promotion_discount_pct_p50 nulls first;
