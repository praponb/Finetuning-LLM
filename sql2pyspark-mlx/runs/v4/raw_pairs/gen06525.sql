select
    s.id,
    s.status,
    s.cost,
    s.shipped_date
from shipments s
where s.order_id in (select id from orders where amount between 790 and 910);
