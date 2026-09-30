select id, category, stock_qty, case when price >= 1060 then 'High' when price >= 240 then 'Medium' else 'Low' end as price_level from products where cost > 550 order by id desc
