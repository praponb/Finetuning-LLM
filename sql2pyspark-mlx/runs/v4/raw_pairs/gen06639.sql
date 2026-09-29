with enriched as (
    select
        *,
        percent_rank() over (partition by promo_code order by discount_pct desc) as discount_pct_percent_rank,
        avg(discount_pct) over (partition by channel order by discount_pct, id rows between 3 preceding and current row) as discount_pct_avg_3
    from promotions
),
scored as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_percent_rank,
        discount_pct_avg_3,
        row_number() over (partition by promo_code, channel order by discount_pct_avg_3 desc, id) as row_seq,
        sum(discount_pct_avg_3) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as cumulative_discount_pct_avg_3
    from enriched
),
with_window as (
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from scored a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.ends_on >= pr.starts_on
       and a.ends_on <= date_add(pr.ends_on, 7)
),
ranked as (
    select *
    from with_window
    qualify row_number() over (partition by promotion_promo_code order by discount_pct, id, promo_id) = 1
),
base as (
    select
        *,
        case when discount_pct >= 0.27 then 'High' when discount_pct >= 0.14 then 'Medium' else 'Low' end as discount_pct_level,
        coalesce(channel, 'Unknown') as channel_filled,
        coalesce(promotion_promo_code, 'UNKNOWN') as promotion_promo_code_filled
    from ranked
    where ends_on >= date '2024-02-08'
      and promotion_promo_code = 'VIP'
),
matched as (
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from base s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on <= date_add(pr.ends_on, 7)
),
deduped as (
    select *
    from matched
    qualify row_number() over (partition by promotion_promo_code, channel order by discount_pct_avg_3 desc nulls last, id, promo_id, promo_id_2) = 1
),
pivoted as (
    select *
    from (
        select
            promotion_promo_code_2,
            channel,
            promo_code,
            promo_id
        from deduped
    ) src
    pivot (
        sum(promo_id)
        for promo_code in ('VIP', 'WELCOME5', 'SPRING10')
    )
)
select
    promotion_promo_code_2,
    channel,
    VIP,
    SPRING10
from pivoted
order by channel desc nulls first, promotion_promo_code_2 desc
