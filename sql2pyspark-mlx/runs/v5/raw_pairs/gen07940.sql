SELECT
    id,
    stock_qty,
    launch_date,
    SUM(cost) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_cost,
    COUNT(cost) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_cost,
    AVG(cost) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) cost_share
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale')
