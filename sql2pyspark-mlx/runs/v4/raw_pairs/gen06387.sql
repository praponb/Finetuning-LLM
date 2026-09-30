WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        YEAR(opened_date) AS opened_date_year
    FROM accounts
    WHERE opened_date > DATE '2023-09-07'
      AND status IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY status, customer_id ORDER BY opened_date_year DESC) AS row_num
    FROM base
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date_year ASC NULLS LAST) AS opened_date_year_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY opened_date_year ASC NULLS FIRST) AS opened_date_year_percent_rank
    FROM deduped
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(opened_date_year_dense_rank, 2) AS opened_date_year_dense_rank_rounded
    FROM metrics
    WHERE status IS NOT NULL
)
SELECT
    id,
    account_type,
    opened_date,
    status,
    opened_date_year_dense_rank,
    opened_date_year_percent_rank,
    opened_date_year_dense_rank_rounded,
    CASE WHEN account_type RLIKE '^(LOAN|CREDIT)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS account_type_zone
FROM valid
ORDER BY opened_date_year_dense_rank ASC NULLS FIRST, status ASC;
