select *, case when unit_price >= 300 then 'High' when unit_price >= 80 then 'Medium' else 'Low' end as unit_price_tier, round(unit_price, 0) as unit_price_rounded
from order_items
where unit_price < 290 and quantity <= 5
