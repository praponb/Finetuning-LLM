WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        unit_price - quantity AS line_total,
        quantity * unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        GREATEST(line_total, unit_price) AS greatest_line_total_unit_price
    FROM filtered
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY greatest_line_total_unit_price DESC, id ASC) AS event_seq,
        AVG(greatest_line_total_unit_price) OVER (ORDER BY greatest_line_total_unit_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS greatest_line_total_unit_price_avg_6,
        AVG(greatest_line_total_unit_price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS greatest_line_total_unit_price_avg_5_prior
    FROM cleaned
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        greatest_line_total_unit_price,
        event_seq,
        greatest_line_total_unit_price_avg_6,
        greatest_line_total_unit_price_avg_5_prior,
        COUNT(greatest_line_total_unit_price) OVER (PARTITION BY order_id) AS order_id_count_greatest_line_total_unit_price,
        LAG(greatest_line_total_unit_price_avg_6, 1) OVER (PARTITION BY order_id, product_id ORDER BY greatest_line_total_unit_price_avg_6 ASC, id ASC) AS prev_greatest_line_total_unit_price_avg_6,
        FIRST_VALUE(greatest_line_total_unit_price_avg_5_prior, TRUE) OVER (PARTITION BY order_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_line_total_unit_price_avg_5_prior,
        AVG(greatest_line_total_unit_price_avg_6) OVER (ORDER BY greatest_line_total_unit_price_avg_5_prior ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS greatest_line_total_unit_price_avg_6_avg_6_prior
    FROM enriched
)
-- Grouped metrics
SELECT
    order_id,
    product_id,
    CASE WHEN greatest_line_total_unit_price_avg_6 >= 77 THEN 'HIGH' ELSE 'LOW' END AS greatest_line_total_unit_price_avg_6_tier,
    MAX(line_total) AS max_line_total,
    PERCENTILE_APPROX(greatest_line_total_unit_price_avg_6_avg_6_prior, ARRAY(0.25, 0.5, 0.75)) AS greatest_line_total_unit_price_avg_6_avg_6_prior_quartiles
FROM scored
GROUP BY order_id, product_id, CASE WHEN greatest_line_total_unit_price_avg_6 >= 77 THEN 'HIGH' ELSE 'LOW' END
HAVING max_line_total < 0;
