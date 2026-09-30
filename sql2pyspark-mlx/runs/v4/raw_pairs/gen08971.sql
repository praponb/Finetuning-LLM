with valid as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), starts_on) as days_since_starts_on
    from promotions
    where starts_on > date '2024-02-18'
      and channel is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(days_since_starts_on) over (partition by promo_code order by ends_on, id rows between 3 preceding and current row) as days_since_starts_on_avg_3,
        sum(discount_pct) over (order by discount_pct, id rows between unbounded preceding and current row) as running_discount_pct,
        sum(discount_pct) over (partition by promo_code order by starts_on, id rows between unbounded preceding and current row) as running_discount_pct_2
    from valid
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        first_value(channel, true) over (order by discount_pct, id rows between unbounded preceding and current row) as first_known_channel,
        cume_dist() over (order by discount_pct desc) as discount_pct_cume_dist
    from metrics
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows a
    left join promotions pr
        on a.channel = pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on <= date_add(pr.ends_on, 7)
),
valid_promotions as (
    -- Filter the raw rows and derive helper columns
    select *, days_since_starts_on_avg_3 - promotion_discount_pct as days_since_starts_on_avg_3_minus_promotion_discount_pct
    from with_window
    where promotion_promo_code is not null
      and promotion_promo_code not in ('BUNDLE', 'SPRING10')
),
metrics_promotions as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        days_since_starts_on,
        days_since_starts_on_avg_3,
        running_discount_pct,
        running_discount_pct_2,
        first_known_channel,
        discount_pct_cume_dist,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        days_since_starts_on_avg_3_minus_promotion_discount_pct,
        avg(days_since_starts_on) over (partition by promo_code order by ends_on, id, promo_id rows between 6 preceding and 1 preceding) as days_since_starts_on_avg_6_prior,
        dense_rank() over (partition by promotion_promo_code, promo_code order by running_discount_pct_2 nulls last) as running_discount_pct_2_dense_rank,
        avg(days_since_starts_on_avg_3) over (order by discount_pct_cume_dist, id, promo_id rows between 2 preceding and 1 preceding) as days_since_starts_on_avg_3_avg_2_prior
    from valid_promotions
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from metrics_promotions s
    inner join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval '7' day
)
select
    id,
    starts_on,
    ends_on,
    discount_pct,
    days_since_starts_on_avg_3,
    running_discount_pct,
    running_discount_pct_2,
    discount_pct_cume_dist,
    promo_id
from with_window_promotions
order by running_discount_pct;
