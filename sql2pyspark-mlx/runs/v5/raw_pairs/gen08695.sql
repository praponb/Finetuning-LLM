with base as (
    -- Keep recent, valid rows
    select
        *,
        concat(channel, '/', promo_code) as channel_promo_code,
        coalesce(channel, 'N/A') as channel_filled
    from promotions
    where channel is not null
),
latest as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by promo_code order by starts_on, id) = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from latest s
    left join promotions pr
        on coalesce(s.channel, 'store') = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval '14' day
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        ends_on,
        channel_promo_code,
        channel_filled,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        ntile(10) over (partition by promotion_promo_code order by discount_pct, id, promo_id) as discount_pct_bucket,
        sum(discount_pct) over (partition by channel, promo_code order by starts_on, id, promo_id rows between unbounded preceding and current row) as running_discount_pct
    from matched
    where promo_id between 6 and 8
),
matched_promotions as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code,
        pr.discount_pct
    from enriched s
    left join promotions pr
        on coalesce(s.channel, 'store') = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 7)
)
select
    id,
    channel,
    ends_on,
    channel_filled,
    promo_id,
    promotion_discount_pct,
    running_discount_pct,
    promo_id_2,
    promo_code
from matched_promotions
order by running_discount_pct desc
