select *
from (
    select
        promo_code,
        channel,
        discount_pct
    from promotions
) src
pivot (
    avg(discount_pct) as value_sum,
    count(1) as n
    for channel in ('mobile', 'partner')
);
