with filtered as (
    select
        *,
        date_trunc('month', ends_on) as ends_on_period,
        trim(channel) as clean_channel
    from promotions
    where starts_on > '2023-03-01'
),
matched as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from filtered e
    join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on <= date_add(pr.ends_on, 3)
),
deduped as (
    select *, row_number() over (partition by channel, promotion_promo_code order by discount_pct desc, id, promo_id) as row_num
    from matched
    qualify row_num = 1
),
with_window as (
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from deduped src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.starts_on >= pr.starts_on
       and src.starts_on <= date_add(pr.ends_on, 10)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by promo_code order by promotion_discount_pct_2 nulls last, id, promo_id, promo_id_2) as row_num_2
        from with_window
    ) t
    where row_num_2 = 1
),
base as (
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        clean_channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        row_num,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        row_num_2,
        coalesce(promo_id_2, 0) as promo_id_2_filled,
        coalesce(channel, 'OTHER') as channel_filled
    from ranked
    where ends_on > date '2023-06-29'
),
matched_promotions as (
    select
        s.*,
        pr.id as promo_id_3,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct_3
    from base s
    join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 3)
),
grouped as (
    select
        date_trunc('quarter', ends_on) as quarter_start,
        count(*) as cnt,
        count(case when promotion_promo_code = 'WELCOME5' then 1 end) as welcome5_count
    from matched_promotions
    group by quarter_start
    having welcome5_count >= 1
)
select quarter_start, welcome5_count
from grouped
order by welcome5_count desc nulls last;
