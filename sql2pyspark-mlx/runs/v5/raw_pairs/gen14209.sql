WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
        ROUND(cost, 1) AS cost_rounded
    FROM shipments
    WHERE delivered_date > TO_DATE('2023-07-17')
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, o.discount
    FROM cleaned s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
cleaned_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date_2
    FROM enriched
    WHERE delivered_date > DATE '2023-04-21'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        days_delivered_date_to_shipped_date,
        cost_rounded,
        discount,
        days_delivered_date_to_shipped_date_2,
        LAST_DAY(shipped_date) AS shipped_date_month_end,
        INITCAP(TRIM(carrier)) AS carrier_title
    FROM cleaned_shipments
    WHERE shipped_date >= TO_DATE('2023-08-24')
      AND order_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        days_delivered_date_to_shipped_date,
        cost_rounded,
        days_delivered_date_to_shipped_date_2,
        carrier_title,
        LAG(days_delivered_date_to_shipped_date_2) OVER (PARTITION BY carrier ORDER BY shipped_date_month_end, id) AS prev_days_delivered_date_to_shipped_date_2,
        LEAD(discount) OVER (PARTITION BY carrier ORDER BY shipped_date_month_end, id) AS next_discount,
        LAG(discount) OVER (PARTITION BY carrier ORDER BY shipped_date_month_end, id) AS prev_discount,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date_month_end, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_rounded DESC, id) AS event_seq
    FROM valid
    WHERE delivered_date <= '2024-07-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, cost_rounded + row_seq AS cost_rounded_plus_row_seq
    FROM metrics
    WHERE shipped_date >= DATE '2023-08-02'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id
    FROM shipments
    WHERE delivered_date BETWEEN '2023-12-01' AND '2024-02-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.id = k.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(event_seq, 0.95) AS event_seq_p95,
        MAX(days_delivered_date_to_shipped_date) AS max_days_delivered_date_to_shipped_date,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(days_delivered_date_to_shipped_date), 2) AS avg_days_delivered_date_to_shipped_date
    FROM kept_rows
    GROUP BY 1
    HAVING APPROX_PERCENTILE(event_seq, 0.95) < 1000000
)
SELECT
    quarter_start,
    row_count,
    event_seq_p95,
    max_days_delivered_date_to_shipped_date,
    avg_days_delivered_date_to_shipped_date
FROM stats;
