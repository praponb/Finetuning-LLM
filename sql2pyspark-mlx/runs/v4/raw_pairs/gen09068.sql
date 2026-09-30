select id, unit_price
from order_items
where unit_price >= (select avg(unit_price) from order_items)
order by id, 2 desc;
