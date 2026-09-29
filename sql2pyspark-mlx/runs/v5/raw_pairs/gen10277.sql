select *
from (
    select
        status,
        channel,
        discount
    from orders
) src
pivot (
    sum(discount)
    for channel in ('mobile' as mobile, 'store' as store)
);
