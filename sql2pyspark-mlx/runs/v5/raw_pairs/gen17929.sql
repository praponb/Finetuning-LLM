WITH cleaned AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
enriched AS (
    SELECT
        src.id,
        src.order_id,
        src.quantity,
        src.unit_price,
        src.line_total,
        p.channel,
        p.amount,
        p.discount
    FROM cleaned src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
scored AS (
    SELECT
        *,
        LAG(discount) OVER (PARTITION BY order_id, channel ORDER BY line_total ASC, id ASC) AS prev_discount,
        LAG(amount) OVER (PARTITION BY order_id, channel ORDER BY line_total ASC, id ASC) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY order_id, channel ORDER BY line_total ASC, id ASC) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id, channel ORDER BY line_total ASC, id ASC) AS seq_no,
        SUM(line_total) OVER (PARTITION BY channel ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_line_total
    FROM enriched
    WHERE unit_price BETWEEN 340 AND 370
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        line_total,
        amount,
        discount,
        prev_discount,
        prev_amount,
        next_amount,
        LAST_VALUE(next_amount) IGNORE NULLS OVER (PARTITION BY order_id, channel ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_amount,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount DESC NULLS LAST) AS discount_percent_rank,
        AVG(discount) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        LEAD(unit_price, 1) OVER (PARTITION BY channel ORDER BY amount ASC, id ASC) AS next_unit_price,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM scored
)
SELECT
    CASE WHEN discount_percent_rank >= 0.62 THEN 'HIGH' ELSE 'LOW' END AS discount_percent_rank_tier,
    order_id,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    PERCENTILE_APPROX(discount_avg_6, ARRAY(0.25, 0.5, 0.75)) AS discount_avg_6_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN discount_percent_rank >= 0.62 THEN 'HIGH' ELSE 'LOW' END ORDER BY CORR(quantity, unit_price) DESC) AS corr_quantity_unit_price_percent_rank
FROM enriched_order_items
GROUP BY 1, 2
HAVING corr_quantity_unit_price > 0
ORDER BY discount_percent_rank_tier ASC NULLS FIRST;
