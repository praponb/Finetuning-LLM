with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        last_day(ends_on) as ends_on_month_end,
        dayofweek(ends_on) as ends_on_dayofweek,
        coalesce(channel, 'OTHER') as channel_clean
    from promotions
    where starts_on >= date '2023-11-04'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by promo_code order by starts_on desc, id) as rn
        from valid
    ) ranked
    where rn = 1
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from ranked a
    left join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
)
-- Grouped metrics
select
    date_trunc('quarter', ends_on) as quarter_start,
    promotion_promo_code,
    channel,
    sum(case when promo_code = 'WELCOME5' then ends_on_dayofweek else 0 end) as welcome5_ends_on_dayofweek,
    round(sum(ends_on_dayofweek) / nullif(count(case when channel = 'web' then 1 end), 0), 2) as ends_on_dayofweek_per_web,
    size(collect_set(promotion_promo_code)) as num_promotion_promo_codes,
    size(collect_set(promotion_promo_code)) as num_promotion_promo_codes_2,
    rank() over (partition by date_trunc('quarter', ends_on) order by size(collect_set(promotion_promo_code)) desc) as num_promotion_promo_codes_2_rank
from with_window
group by 1, 2, 3
order by ends_on_dayofweek_per_web nulls last
