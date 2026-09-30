select *
from (
    select
        id,
        channel,
        shipping_fee,
        row_number() over (partition by channel order by shipping_fee desc, id) as row_num
    from orders
    where amount is not null
) as ranked
where row_num <= 2;
