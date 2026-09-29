WITH exploded AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
base AS (
    SELECT
        *,
        LENGTH(value) AS value_length,
        COALESCE(value, 'N/A') AS value_filled,
        COALESCE(value, 'NONE') AS value_filled_2
    FROM exploded
    WHERE value IN ('WELCOME5', 'BUNDLE', 'SPRING10')
),
filtered AS (
    SELECT
        *,
        ROUND(value_length * 1.1, 2) AS value_length_adjusted,
        COALESCE(value, 'NONE') AS value_clean
    FROM base
    WHERE value = 'SPRING10'
),
scored AS (
    SELECT
        *,
        MIN(value_length_adjusted) OVER (PARTITION BY value, customer_id) AS value_min_value_length_adjusted,
        PERCENT_RANK() OVER (ORDER BY value_length NULLS LAST) AS value_length_percent_rank,
        RANK() OVER (ORDER BY value_length DESC NULLS FIRST) AS value_length_rank,
        RANK() OVER (ORDER BY value_length_adjusted DESC NULLS FIRST) AS value_length_adjusted_rank,
        MAX(value_length) OVER (PARTITION BY customer_id) AS customer_id_max_value_length
    FROM filtered
    WHERE value_length <> 43
),
wide AS (
    SELECT *
    FROM (
        SELECT
            customer_id,
            value,
            value_length_adjusted_rank
        FROM scored
    ) src
    PIVOT (
        SUM(value_length_adjusted_rank) AS value_sum,
        COUNT(1) AS item_count
        FOR value IN ('BUNDLE', 'SPRING10')
    )
)
SELECT
    customer_id,
    BUNDLE_value_sum,
    BUNDLE_item_count,
    SPRING10_value_sum
FROM wide
ORDER BY SPRING10_value_sum NULLS FIRST;
