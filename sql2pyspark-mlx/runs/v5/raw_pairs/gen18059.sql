WITH with_windows AS (
    SELECT
        *,
        COUNT(amount) OVER w_recent AS last3_count_amount,
        MIN(amount) OVER w_recent AS last3_min_amount,
        AVG(amount) OVER w_recent AS last3_avg_amount,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_cume_dist
    FROM transactions
    WINDOW w_recent AS (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM transactions
    WHERE account_id IN (4, 22, 28, 34)
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    SELECT
        merchant,
        is_flagged,
        RANK() OVER w_recent AS amount_cume_dist_rank,
        COUNT(amount) OVER w_running AS amount_count_7d
    FROM verified
    WHERE last3_count_amount < 6
    WINDOW w_recent AS (ORDER BY amount_cume_dist DESC NULLS LAST), w_running AS (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM scored
    WHERE amount_cume_dist_rank < 8
),
base AS (
    SELECT
        merchant,
        is_flagged,
        amount_count_7d,
        is_flagged_label,
        amount_cume_dist_rank + amount_count_7d AS amount_cume_dist_rank_plus_amount_count_7d,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM filtered
    WHERE merchant IS NOT NULL
)
SELECT
    merchant,
    is_flagged,
    amount_count_7d,
    is_flagged_label,
    amount_cume_dist_rank_plus_amount_count_7d,
    CASE WHEN merchant RLIKE '^(Starbucks|Amazon)$' THEN 'PRIORITY' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS merchant_zone
FROM base
ORDER BY amount_count_7d, merchant DESC
