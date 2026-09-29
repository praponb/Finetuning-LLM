WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-07-08'
      AND status IS DISTINCT FROM 'OPEN'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC NULLS FIRST, id ASC) = 1
),
cleaned_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_ADD(opened_date, 81) AS opened_date_plus_81d,
        CEIL(balance) AS balance_whole,
        COALESCE(status, 'OTHER') AS status_clean
    FROM latest
    WHERE status IS NOT DISTINCT FROM 'CLOSED'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date ASC NULLS FIRST, id ASC) AS rank_in_group
        FROM cleaned_accounts
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        account_type,
        opened_date,
        days_since_opened_date,
        status_filled,
        opened_date_plus_81d,
        status_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_whole ASC, id ASC) AS dedup_rank
        FROM deduped
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    account_type,
    opened_date,
    status_filled,
    opened_date_plus_81d,
    status_clean
FROM ranked
ORDER BY opened_date_plus_81d ASC;
