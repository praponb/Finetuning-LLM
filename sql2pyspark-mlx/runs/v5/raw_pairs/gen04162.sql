select id, channel
from (
    select *, row_number() over (partition by channel order by amount desc, id) as row_num
    from orders
) ranked
where row_num = 1
