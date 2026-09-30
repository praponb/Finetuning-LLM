WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END is_enterprise
    FROM subscriptions
    WHERE monthly_fee >= 130
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.end_date,
        e.status,
        c.country,
        c.segment
    FROM filtered e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY end_date DESC, id ASC) row_seq
    FROM joined
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.country AS customer_country,
        c.signup_date
    FROM with_windows src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        end_date,
        status,
        segment,
        row_seq,
        COALESCE(segment, 'N/A') segment_filled,
        DATE_FORMAT(signup_date, 'MMM yyyy') signup_date_label,
        COALESCE(segment, 'UNKNOWN') segment_clean
    FROM with_dims
    WHERE YEAR(end_date) >= 2024
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(status, 1, 3) status_prefix
    FROM valid
    WHERE end_date > TO_DATE('2023-11-15')
      AND segment IS NOT NULL
)
SELECT
    id,
    end_date,
    status,
    row_seq,
    segment_filled,
    signup_date_label,
    segment_clean,
    status_prefix,
    CASE WHEN status RLIKE '^(EXPIRED|PAUSED)$' THEN 'CORE' WHEN status RLIKE '^(ACTIVE|CANCELLED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone
FROM base
