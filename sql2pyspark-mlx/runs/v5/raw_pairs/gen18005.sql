with with_windows as (
    select
        *,
        ntile(10) over (partition by channel order by discount_pct desc, id) as discount_pct_bucket,
        sum(discount_pct) over (order by starts_on, id rows between unbounded preceding and current row) as cumulative_discount_pct
    from promotions
    where discount_pct <= 0.27
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by channel order by cumulative_discount_pct desc nulls first, id) as rn
        from with_windows
    ) t
    where rn = 1
),
attributed as (
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from deduped src
    left join promotions pr
        on src.channel = pr.channel
       and src.ends_on >= pr.starts_on
       and src.ends_on <= date_add(pr.ends_on, 3)
),
with_windows_promotions as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_bucket,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        ntile(10) over (order by discount_pct, id, promo_id) as discount_pct_bucket_2,
        lag(discount_pct, 1) over (partition by promotion_promo_code order by discount_pct, id, promo_id) as prev_discount_pct
    from attributed
),
filtered as (
    select
        *,
        round(prev_discount_pct / promo_id, 2) as prev_discount_pct_per_promo_id,
        discount_pct + discount_pct_bucket_2 as discount_pct_plus_discount_pct_bucket_2
    from with_windows_promotions
    where ends_on > date '2023-07-02'
),
ranked as (
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_bucket,
        promo_id,
        promotion_discount_pct,
        discount_pct_bucket_2
    from filtered
    qualify row_number() over (partition by channel order by discount_pct_bucket, id, promo_id) = 1
),
attributed_promotions as (
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct_2
    from ranked s
    left join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 3)
)
select
    date_trunc('year', ends_on) as year_start,
    count(*) as row_count,
    count_if(channel = 'partner') as partner_events,
    count_if(promo_code = 'VIP') as vip_events,
    sum(promo_id_2) as total_promo_id_2,
    max(promotion_discount_pct_2) as max_promotion_discount_pct_2
from attributed_promotions
group by date_trunc('year', ends_on)
having vip_events > 1;
