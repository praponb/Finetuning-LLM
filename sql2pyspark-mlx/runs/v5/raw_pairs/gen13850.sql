WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        discount,
        GREATEST(discount, amount) AS greatest_discount_amount,
        shipping_fee + amount AS amount_with_shipping
    FROM orders
    WHERE store_id IS NOT NULL
      AND channel IS NULL
)
-- Running and moving aggregates
SELECT
    *,
    SUM(greatest_discount_amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_greatest_discount_amount,
    MIN(greatest_discount_amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_greatest_discount_amount,
    AVG(greatest_discount_amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_greatest_discount_amount,
    COUNT(greatest_discount_amount) OVER (PARTITION BY customer_id, store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_greatest_discount_amount
FROM valid;
