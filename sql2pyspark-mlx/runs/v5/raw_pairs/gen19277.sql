WITH enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.account_type,
        src.opened_date,
        src.status,
        c.country
    FROM accounts src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
enriched_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY country, account_type ORDER BY opened_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER w_running AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS seq_no_2
    FROM enriched
    WINDOW w_running AS (PARTITION BY account_type, country ORDER BY opened_date DESC, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM enriched_accounts
    INTERSECT
    SELECT id, customer_id
    FROM accounts
    WHERE balance IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_accounts s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        LEAST(row_seq, seq_no) AS least_row_seq_seq_no
    FROM reconciled
    WHERE customer_id IS NOT NULL
      AND customer_id IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    d.segment,
    a.account_type,
    COUNT(*) AS rows_total,
    MAX(a.row_seq) AS max_row_seq
FROM base a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY CUBE (d.segment, a.account_type);
