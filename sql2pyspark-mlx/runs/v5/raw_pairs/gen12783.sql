WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        account_id,
        txn_time,
        merchant
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC NULLS FIRST, id) AS row_num
        FROM transactions
    ) x
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        merchant,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY account_id, merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant_2
    FROM ranked
    WHERE txn_time >= '2024-11-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(merchant) AS merchant_length
    FROM enriched
    WHERE merchant IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(merchant) AS merchant_length_2,
        COALESCE(merchant_length, 0) AS merchant_length_filled,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM filtered
    WHERE merchant IN ('Amazon', '7-Eleven')
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN merchant_length_2 >= 80 THEN 'LARGE' ELSE 'SMALL' END AS merchant_length_2_tier,
        COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count,
        APPROX_PERCENTILE(merchant_length_filled, 0.9) AS merchant_length_filled_p90
    FROM cleaned
    GROUP BY CASE WHEN merchant_length_2 >= 80 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    merchant_length_2_tier,
    amazon_count,
    merchant_length_filled_p90
FROM daily
ORDER BY merchant_length_filled_p90;
