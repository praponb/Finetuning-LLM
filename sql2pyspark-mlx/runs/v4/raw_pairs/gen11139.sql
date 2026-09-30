select category, supplier_id, sum(cost) total_cost from products group by category, supplier_id;
