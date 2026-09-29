select unit_price, case when unit_price >= 400 then 'High' when unit_price >= 80 then 'Medium' else 'Low' end as unit_price_level
from order_items
where unit_price < 170
order by unit_price desc;
