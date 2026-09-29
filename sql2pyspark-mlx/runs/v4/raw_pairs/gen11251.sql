WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        LENGTH(country) AS country_length
    FROM sessions
    WHERE started_at >= DATE '2023-04-11'
      AND device = 'mobile'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY country_length DESC NULLS LAST, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(country_length) IGNORE NULLS OVER w_running AS first_known_country_length,
        MIN(country_length) OVER (PARTITION BY customer_id) AS customer_id_min_country_length
    FROM ranked
    WINDOW w_running AS (ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    SELECT e.*, b.age
    FROM with_windows e
    INNER JOIN customers b
        ON e.customer_id = b.id
)
SELECT
    *,
    NTILE(5) OVER (PARTITION BY country ORDER BY age DESC, id) AS age_bucket,
    MIN(first_known_country_length) OVER (PARTITION BY customer_id) AS customer_id_min_first_known_country_length
FROM with_dims
