SELECT
    category,
    brand,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY BIT_OR(stock_qty) DESC) AS stock_qty_bit_or_dense_rank
FROM products
GROUP BY category, brand;
