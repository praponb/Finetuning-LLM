select *, case when unit_price >= 370 then 'High' when unit_price >= 170 then 'Medium' else 'Low' end unit_price_level, round(unit_price, 1) unit_price_rounded
from order_items
where unit_price between 260 and 300;
