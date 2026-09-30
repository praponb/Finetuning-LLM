WITH base AS (
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM transactions
    WHERE txn_time > TO_DATE('2023-04-09')
      AND account_id = 2
),
remaining_keys AS (
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM transactions
    WHERE is_flagged
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.id = k.id
),
filtered AS (
    SELECT *, amount / NULLIF(amount_filled, 0) AS amount_per_amount_filled
    FROM verified
    WHERE amount_filled <= 57
),
base_transactions AS (
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        amount_filled,
        merchant_filled,
        amount_per_amount_filled,
        ROUND(amount_filled, 0) AS amount_filled_rounded
    FROM filtered
    WHERE merchant IN ('Shell', 'Starbucks', 'Amazon')
),
cleaned AS (
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        amount_filled,
        merchant_filled,
        amount_per_amount_filled,
        amount_filled_rounded,
        ROUND(amount_per_amount_filled / amount, 2) AS amount_per_amount_filled_per_amount,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM base_transactions
    WHERE NOT is_flagged
),
grouped AS (
    SELECT
        CASE WHEN amount_per_amount_filled_per_amount >= 81 THEN 'LARGE' ELSE 'SMALL' END AS amount_per_amount_filled_per_amount_tier,
        CASE WHEN amount_filled_rounded >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_filled_rounded_tier,
        ROUND(AVG(amount_filled_rounded), 2) AS avg_amount_filled_rounded,
        COUNT(*) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_rows,
        APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
        PERCENTILE_APPROX(amount_filled, ARRAY(0.25, 0.5, 0.75)) AS amount_filled_quartiles,
        COUNT(*) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_rows_2,
        GROUPING_ID() AS grouping_level
    FROM cleaned
    GROUP BY GROUPING SETS ((CASE WHEN amount_per_amount_filled_per_amount >= 81 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount_filled_rounded >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN amount_per_amount_filled_per_amount >= 81 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    amount_filled_rounded_tier,
    avg_amount_filled_rounded,
    7_eleven_rows,
    amount_filled_quartiles,
    7_eleven_rows_2,
    grouping_level
FROM grouped
ORDER BY amount_filled_rounded_tier DESC NULLS LAST;
