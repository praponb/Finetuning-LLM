WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        FLOOR(amount) AS amount_whole,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE is_flagged = TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST, id) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount_whole, 1) OVER (PARTITION BY txn_type, account_id ORDER BY amount_whole, id) AS next_amount_whole,
        CUME_DIST() OVER w_running AS amount_whole_cume_dist,
        MAX(amount) OVER (PARTITION BY txn_type) AS txn_type_max_amount,
        LAG(amount, 1) OVER (PARTITION BY account_id, txn_type ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount_whole DESC, id) AS seq_no
    FROM latest
    WHERE amount_whole <> 43
    WINDOW w_running AS (ORDER BY amount_whole DESC NULLS LAST)
),
cleaned_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        amount_whole,
        resolved_merchant,
        rn,
        txn_type_max_amount,
        prev_amount,
        LEAST(amount_whole_cume_dist, next_amount_whole) AS least_amount_whole_cume_dist_next_amount_whole
    FROM enriched
    WHERE account_id IN (13, 29, 33, 34)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        amount_whole,
        resolved_merchant,
        rn,
        txn_type_max_amount,
        least_amount_whole_cume_dist_next_amount_whole,
        CEIL(rn) AS rn_whole
    FROM cleaned_transactions
    WHERE amount_whole >= 44
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, rn / NULLIF(rn_whole, 0) AS rn_per_rn_whole
    FROM filtered
    WHERE rn_whole >= 41
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        amount_whole,
        resolved_merchant,
        rn,
        least_amount_whole_cume_dist_next_amount_whole,
        rn_whole,
        LAG(rn_whole, 1) OVER w_grp AS prev_rn_whole,
        CUME_DIST() OVER w_part AS least_amount_whole_cume_dist_next_amount_whole_cume_dist,
        DENSE_RANK() OVER w_grp_2 AS least_amount_whole_cume_dist_next_amount_whole_dense_rank
    FROM valid
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY amount_whole, id), w_part AS (PARTITION BY account_id ORDER BY least_amount_whole_cume_dist_next_amount_whole DESC), w_grp_2 AS (PARTITION BY account_id ORDER BY least_amount_whole_cume_dist_next_amount_whole DESC NULLS FIRST)
)
SELECT
    id,
    amount,
    amount_whole,
    resolved_merchant,
    rn,
    least_amount_whole_cume_dist_next_amount_whole,
    rn_whole,
    prev_rn_whole,
    least_amount_whole_cume_dist_next_amount_whole_cume_dist
FROM with_windows
