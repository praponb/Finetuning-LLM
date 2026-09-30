SELECT
    id,
    rating,
    country,
    supplier_name,
    DENSE_RANK() OVER (ORDER BY rating DESC) AS dense_rnk,
    LAST_VALUE(supplier_name) OVER (PARTITION BY country ORDER BY rating DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_supplier_name
FROM suppliers
