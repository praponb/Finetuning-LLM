WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        CAST(amount AS INT) AS amount_int,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-03-26 20:00:00'
      AND merchant IS NOT NULL
)
-- Attach accounts attributes
SELECT
    s.*,
    p.account_type,
    p.opened_date
FROM valid s
LEFT JOIN accounts p
    ON s.account_id = p.id;
