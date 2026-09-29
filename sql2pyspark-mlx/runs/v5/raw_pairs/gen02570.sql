with ranked as (
    -- QUALIFY-style window filter
    select *
    from promotions
    qualify row_number() over (partition by promo_code, channel order by discount_pct desc, id) = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ntile(10) over (partition by channel order by discount_pct, id) as discount_pct_bucket,
        last_value(channel, true) over (partition by channel order by ends_on, id rows between unbounded preceding and current row) as last_known_channel,
        lead(discount_pct, 2) over (partition by promo_code order by starts_on, id) as next_discount_pct
    from ranked
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on <= date_add(pr.ends_on, 7)
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from with_window a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on <= date_add(pr.ends_on, 10)
)
select
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_bucket,
    last_known_channel,
    promo_id,
    promotion_promo_code
from matched
order by promo_code desc
