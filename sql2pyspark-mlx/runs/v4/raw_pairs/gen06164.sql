select supplier_id, category, max(cost) max_cost from products where supplier_id in (5, 16, 17, 21) group by 1, 2 order by category, supplier_id limit 5;
