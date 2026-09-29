select id, discount, order_date, channel, case when discount >= 0.35 then 'High' when discount >= 0.1 then 'Medium' else 'Low' end discount_level from orders order by discount, id desc limit 10;
