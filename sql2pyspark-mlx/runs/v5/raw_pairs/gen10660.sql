select id, order_id, shipped_date
from (
    select *, row_number() over (partition by order_id order by shipped_date desc, id) rn
    from shipments
) ranked
where rn = 1
order by shipped_date;
