with ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by promo_code order by starts_on desc) as row_num
    from promotions
    qualify row_num = 1
)
select
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    row_num
from ranked
order by discount_pct nulls last, row_num desc;
