SELECT
    id,
    launch_date,
    brand,
    cost,
    ROW_NUMBER() OVER (ORDER BY launch_date DESC, id) AS launch_date_row,
    DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost) AS cost_dense_rank
FROM products
WHERE category IN ('Books', 'Sports');
