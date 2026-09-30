SELECT * FROM (SELECT id, category, price, cost FROM products) src UNPIVOT (val FOR kpi IN (price, cost))
