select * from (select id, supplier_id, cost, price from products) src unpivot (val for metric in (cost as COST_VAL, price as PRICE_VAL));
