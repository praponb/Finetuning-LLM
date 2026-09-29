with lines as (
    select id, explode(coupon_codes) as code
    from carts
),
valid as (
    select *, case when code in ('WELCOME5', 'BUNDLE') then 'Priority' else 'Other' end code_group
    from lines
    where code is not null
      and code = 'VIP'
),
valid_carts as (
    select
        *,
        substring(code, 1, 4) code_prefix,
        coalesce(code, 'OTHER') code_filled
    from valid
    where code != 'SPRING10'
),
filtered as (
    select
        code_group,
        code_filled,
        upper(code) code_upper,
        case when code in ('BUNDLE', 'WELCOME5') then 'Core' else 'Other' end code_group_2,
        coalesce(code, 'UNKNOWN') code_filled_2
    from valid_carts
    where code is not null
)
select
    code_group,
    code_filled,
    code_upper,
    code_group_2,
    code_filled_2
from filtered;
