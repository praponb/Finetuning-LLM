WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 6) opened_date_plus_months,
        COALESCE(status, 'OTHER') status_filled
    FROM accounts
    WHERE opened_date > TO_DATE('2023-12-23')
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) seq_no,
    ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance, id) event_seq
FROM valid;
