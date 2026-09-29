WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 198 THEN 'High' WHEN quantity >= 97 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE last_restock > DATE '2023-02-13'
),
cleaned_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('YEAR', last_restock) AS last_restock_period
    FROM cleaned
    WHERE last_restock > DATE '2023-03-21'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity_band,
        LAST_VALUE(quantity) IGNORE NULLS OVER w_ordered AS last_known_quantity,
        LAG(quantity, 2) OVER w_hist AS prev_quantity,
        AVG(quantity) OVER (ORDER BY last_restock_period, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior
    FROM cleaned_inventory
    WHERE HOUR(last_restock_period) BETWEEN 7 AND 10
    WINDOW w_ordered AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY quantity, id)
),
with_windows_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last_known_quantity, id) AS seq_no,
        AVG(quantity_avg_2_prior) OVER (ORDER BY quantity_avg_2_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2_prior_avg_2
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity_avg_2_prior_avg_2 >= 206 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_avg_2_prior_avg_2_tier,
    CASE WHEN prev_quantity >= 257 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_quantity_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(last_known_quantity), 2) AS avg_last_known_quantity
FROM with_windows_inventory
GROUP BY CASE WHEN quantity_avg_2_prior_avg_2 >= 206 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_quantity >= 257 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) > 1
ORDER BY quantity_avg_2_prior_avg_2_tier NULLS LAST;
