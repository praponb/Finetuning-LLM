with filtered as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        concat_ws('/', promo_code, channel) as promo_code_channel_key,
        round(discount_pct, 1) as discount_pct_rounded,
        coalesce(channel, 'OTHER') as resolved_channel
    from promotions
    where ends_on > date '2024-05-27'
      and channel is not null
),
with_windows as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_channel_key,
        discount_pct_rounded,
        resolved_channel,
        avg(discount_pct) over (order by discount_pct_rounded, id rows between 5 preceding and 1 preceding) as discount_pct_avg_5_prior,
        percent_rank() over (partition by channel order by discount_pct desc) as discount_pct_percent_rank,
        lead(discount_pct_rounded, 1) over (partition by promo_code order by discount_pct, id) as next_discount_pct_rounded
    from filtered
),
attributed as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows e
    inner join promotions pr
        on e.channel = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on <= date_add(pr.ends_on, 10)
),
with_windows_promotions as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_channel_key,
        discount_pct_rounded,
        resolved_channel,
        discount_pct_avg_5_prior,
        discount_pct_percent_rank,
        next_discount_pct_rounded,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        percent_rank() over (order by discount_pct desc) as discount_pct_percent_rank_2,
        ntile(3) over (partition by promo_code order by discount_pct_rounded desc, id, promo_id) as discount_pct_rounded_bucket,
        sum(promo_id) over (partition by channel order by promotion_discount_pct, id, promo_id rows between unbounded preceding and current row) as cumulative_promo_id
    from attributed
),
latest as (
    select *
    from (
        select *, row_number() over (partition by promotion_promo_code order by cumulative_promo_id nulls first, id, promo_id) as row_num
        from with_windows_promotions
    ) x
    where row_num = 1
),
scored as (
    select
        *,
        row_number() over (partition by promotion_promo_code, promo_code order by promo_id desc, id, promo_id) as event_seq,
        row_number() over (order by discount_pct_rounded_bucket desc, id, promo_id) as seq_no,
        row_number() over (order by cumulative_promo_id desc, id, promo_id) as seq_no_2
    from latest
)
select
    id,
    ends_on,
    discount_pct,
    promo_code_channel_key,
    resolved_channel,
    discount_pct_avg_5_prior,
    discount_pct_percent_rank,
    next_discount_pct_rounded,
    promo_id
from scored;
