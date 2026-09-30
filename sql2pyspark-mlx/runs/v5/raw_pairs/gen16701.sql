select i.id, i.quantity
from inventory i
where i.quantity is not null and i.store_id in (select id from stores where year(opened_date) >= 2023)
order by i.quantity;
