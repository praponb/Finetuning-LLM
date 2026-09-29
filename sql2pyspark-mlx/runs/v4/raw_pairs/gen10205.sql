select * from (select id, supplier_id, price, cost from products) src unpivot (val for kpi in (price as PRICE, cost as COST))
