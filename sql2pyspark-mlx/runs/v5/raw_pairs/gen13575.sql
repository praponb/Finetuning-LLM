WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAST(quantity, unit_price) AS least_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        unit_price,
        least_quantity_unit_price,
        SUM(unit_price) OVER w_hist AS running_unit_price,
        AVG(quantity) OVER w_hist_2 AS quantity_avg_6_prior,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price,
        AVG(least_quantity_unit_price) OVER (PARTITION BY order_id ORDER BY least_quantity_unit_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS least_quantity_unit_price_avg_6
    FROM cleaned
    WINDOW w_hist AS (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(corr_quantity_unit_price) OVER (ORDER BY least_quantity_unit_price_avg_6 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_corr_quantity_unit_price,
        COUNT(corr_quantity_unit_price) OVER (ORDER BY least_quantity_unit_price_avg_6 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_corr_quantity_unit_price,
        MIN(corr_quantity_unit_price) OVER (ORDER BY least_quantity_unit_price_avg_6 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_corr_quantity_unit_price,
        MAX(corr_quantity_unit_price) OVER (ORDER BY least_quantity_unit_price_avg_6 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_corr_quantity_unit_price
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        least_quantity_unit_price,
        running_unit_price,
        quantity_avg_6_prior,
        corr_quantity_unit_price,
        least_quantity_unit_price_avg_6,
        last7_avg_corr_quantity_unit_price,
        last7_min_corr_quantity_unit_price,
        last7_max_corr_quantity_unit_price,
        LEAD(last7_count_corr_quantity_unit_price) OVER (ORDER BY last7_max_corr_quantity_unit_price ASC, id ASC) AS next_last7_count_corr_quantity_unit_price,
        LAG(last7_count_corr_quantity_unit_price) OVER (ORDER BY last7_max_corr_quantity_unit_price ASC, id ASC) AS prev_last7_count_corr_quantity_unit_price,
        LEAD(last7_min_corr_quantity_unit_price) OVER (ORDER BY last7_max_corr_quantity_unit_price ASC, id ASC) AS next_last7_min_corr_quantity_unit_price,
        LAG(last7_min_corr_quantity_unit_price) OVER (ORDER BY last7_max_corr_quantity_unit_price ASC, id ASC) AS prev_last7_min_corr_quantity_unit_price,
        ROW_NUMBER() OVER (ORDER BY last7_max_corr_quantity_unit_price ASC, id ASC) AS seq_no
    FROM enriched
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_unit_price >= 2109 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_unit_price_tier,
        COUNT(*) AS row_count,
        AVG(quantity_avg_6_prior) AS avg_quantity_avg_6_prior,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(running_unit_price, 0.5) AS running_unit_price_p50,
        APPROX_PERCENTILE(last7_avg_corr_quantity_unit_price, 0.5) AS last7_avg_corr_quantity_unit_price_p50
    FROM metrics
    GROUP BY 1
    HAVING AVG(quantity_avg_6_prior) > 0
)
SELECT
    running_unit_price_tier,
    row_count,
    avg_quantity_avg_6_prior,
    running_unit_price_p50
FROM daily
