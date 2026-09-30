SELECT * FROM (SELECT id, category, cost, price FROM products) src UNPIVOT (reading FOR kpi IN (cost AS COST_VAL, price AS PRICE_VAL));
