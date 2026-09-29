with valid as (
    select
        *,
        length(channel) as channel_length,
        concat(channel, ', ', promo_code) as channel_promo_code
    from promotions
    where ends_on between '2023-03-31' and '2024-01-01'
),
latest as (
    select *
    from valid
    qualify row_number() over (partition by promo_code order by discount_pct desc, id) = 1
),
filtered as (
    select *, case when promo_code = 'SPRING10' then 1 else 0 end as is_spring10
    from latest
    where channel_length > 20
)
select
    id,
    promo_code,
    discount_pct,
    channel_length,
    channel_promo_code
from filtered
order by id desc nulls last, channel_length desc;
