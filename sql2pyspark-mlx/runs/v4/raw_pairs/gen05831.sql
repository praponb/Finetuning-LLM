WITH cleaned AS (
    SELECT
        id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 430 THEN 'High' WHEN unit_price >= 70 THEN 'Medium' ELSE 'Low' END unit_price_band,
        CAST(unit_price AS INT) unit_price_int
    FROM order_items
    WHERE unit_price <= 360
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY unit_price_int DESC) unit_price_int_dense_rank,
        AVG(quantity) OVER (ORDER BY unit_price_int, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_avg_3,
        AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) unit_price_avg_2
    FROM cleaned
),
valid AS (
    SELECT
        *,
        unit_price_int * unit_price unit_price_int_times_unit_price,
        CASE WHEN unit_price_int_dense_rank >= 9 THEN 'High' WHEN unit_price_int_dense_rank >= 5 THEN 'Medium' ELSE 'Low' END unit_price_int_dense_rank_band
    FROM metrics
    WHERE unit_price_int = 41
),
enriched AS (
    SELECT
        *,
        LEAD(unit_price, 1) OVER (ORDER BY quantity_avg_3, id) next_unit_price,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price,
        LAG(unit_price_int_times_unit_price, 1) OVER (ORDER BY unit_price_int_dense_rank, id) prev_unit_price_int_times_unit_price
    FROM valid
),
base AS (
    SELECT *, ROUND(unit_price, 1) unit_price_rounded
    FROM enriched
    WHERE quantity_avg_3 < 5
),
filtered AS (
    SELECT
        *,
        CAST(quantity AS STRING) quantity_str,
        unit_price_rounded + unit_price unit_price_rounded_plus_unit_price
    FROM base
    WHERE next_unit_price > 320
),
with_windows AS (
    SELECT
        *,
        SUM(unit_price) OVER (ORDER BY quantity_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        NTILE(3) OVER (ORDER BY unit_price DESC, id) unit_price_bucket,
        AVG(unit_price_int_times_unit_price) OVER (ORDER BY first_known_unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) unit_price_int_times_unit_price_avg_6
    FROM filtered
)
SELECT
    CASE WHEN prev_unit_price_int_times_unit_price >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_unit_price_int_times_unit_price_tier,
    CASE WHEN unit_price_int >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_int_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    PERCENTILE_APPROX(running_unit_price, 0.5) AS running_unit_price_p50
FROM with_windows
GROUP BY 1, 2
ORDER BY distinct_ids_3 DESC
