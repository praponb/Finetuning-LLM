with numbered as (
    select *, row_number() over (partition by order_id order by shipped_date desc, id) as rnk
    from shipments
)
select id, order_id, shipped_date
from numbered
where rnk <= 3
order by order_id asc;
