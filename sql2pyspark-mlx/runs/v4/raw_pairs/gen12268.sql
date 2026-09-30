with valid as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        discount_pct,
        date_add(starts_on, 85) as starts_on_plus_85d
    from promotions
    where starts_on >= to_date('2023-10-14')
      and ends_on < '2023-06-01'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        discount_pct,
        starts_on_plus_85d,
        first_value(channel) ignore nulls over (partition by channel order by starts_on_plus_85d, id rows between unbounded preceding and current row) as first_known_channel,
        cume_dist() over w_ordered as discount_pct_cume_dist,
        percent_rank() over w_ordered as discount_pct_percent_rank,
        lag(discount_pct, 1) over (partition by promo_code order by discount_pct, id) as prev_discount_pct,
        first_value(channel, true) over w_running as first_known_channel_2
    from valid
    window w_ordered as (partition by channel order by discount_pct desc), w_running as (order by starts_on_plus_85d, id rows between unbounded preceding and current row)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by channel, promo_code order by prev_discount_pct desc nulls first) as prev_discount_pct_dense_rank,
        sum(discount_pct_percent_rank) over (order by discount_pct_percent_rank, id rows between unbounded preceding and current row) as cumulative_discount_pct_percent_rank,
        sum(discount_pct_percent_rank) over (partition by channel order by discount_pct_percent_rank, id rows between unbounded preceding and current row) as cumulative_discount_pct_percent_rank_2,
        sum(prev_discount_pct) over (partition by promo_code, channel) as promo_code_sum_prev_discount_pct,
        avg(prev_discount_pct) over (partition by channel order by discount_pct, id rows between 2 preceding and 1 preceding) as prev_discount_pct_avg_2_prior
    from enriched
),
scored_promotions as (
    -- Ranking and gap features
    select
        *,
        sum(prev_discount_pct_dense_rank) over (partition by channel, promo_code order by promo_code_sum_prev_discount_pct, id rows between unbounded preceding and current row) as running_prev_discount_pct_dense_rank,
        first_value(prev_discount_pct_avg_2_prior) ignore nulls over (partition by channel order by prev_discount_pct_avg_2_prior, id rows between unbounded preceding and current row) as first_known_prev_discount_pct_avg_2_prior
    from scored
)
-- Running and moving aggregates
select
    *,
    ntile(3) over (partition by promo_code order by promo_code_sum_prev_discount_pct, id) as promo_code_sum_prev_discount_pct_bucket,
    round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
    row_number() over (partition by channel order by prev_discount_pct desc, id) as event_seq,
    row_number() over (partition by channel order by prev_discount_pct, id) as seq_no,
    ntile(4) over w_running as cumulative_discount_pct_percent_rank_2_bucket
from scored_promotions
window w_running as (partition by channel order by cumulative_discount_pct_percent_rank_2, id);
