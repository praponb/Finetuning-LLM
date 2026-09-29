SELECT *, ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY price DESC NULLS FIRST, id) AS rank_in_group
FROM products
QUALIFY rank_in_group = 1;
