WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END is_grab,
        COALESCE(merchant, 'DIRECT') merchant_filled
    FROM transactions
    WHERE is_flagged
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        b.status,
        b.account_type
    FROM cleaned src
    INNER JOIN accounts b
        ON src.account_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_grab,
        merchant_filled,
        status,
        account_type,
        SUM(is_grab) OVER w_recent cumulative_is_grab,
        COUNT(is_grab) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) is_grab_count_30d,
        LAST_VALUE(is_grab, TRUE) OVER (PARTITION BY txn_type ORDER BY is_grab, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_is_grab
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY last_known_is_grab) last_known_is_grab_rank,
        SUM(is_grab) OVER w_recent account_id_sum_is_grab,
        AVG(is_grab) OVER w_part is_grab_avg_6
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY account_id, status), w_part AS (PARTITION BY merchant, status ORDER BY cumulative_is_grab, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY account_id, status ORDER BY is_grab_avg_6) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        b.opened_date,
        b.status AS account_status,
        b.account_type AS account_account_type
    FROM latest src
    INNER JOIN accounts b
        ON src.account_id = b.id
)
-- Multi-level aggregation
SELECT
    merchant,
    COUNT(CASE WHEN account_account_type = 'CHECKING' THEN 1 END) AS checking_count,
    MIN(txn_time) AS first_txn_time,
    ROUND(AVG(is_grab_count_30d), 2) AS avg_is_grab_count_30d,
    GROUPING_ID() AS gid
FROM enriched
GROUP BY CUBE (merchant);
