WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        FLOOR(cost) AS cost_whole,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND MONTH(shipped_date) IN (1, 2, 10)
),
cleaned_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        cost_whole,
        carrier_filled,
        DATE_ADD(shipped_date, 59) AS shipped_date_plus_59d,
        COALESCE(carrier, 'DIRECT') AS carrier_filled_2
    FROM cleaned
    WHERE delivered_date >= TO_DATE('2023-09-09')
      AND order_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        cost_whole,
        shipped_date_plus_59d,
        SUM(cost_whole) OVER w_hist AS cumulative_cost_whole,
        FIRST_VALUE(cost_whole) IGNORE NULLS OVER w_hist AS first_known_cost_whole,
        RANK() OVER (PARTITION BY carrier ORDER BY cost_whole DESC NULLS LAST) AS cost_whole_rank
    FROM cleaned_shipments
    WINDOW w_hist AS (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cumulative_cost_whole) OVER w_grp AS prev_cumulative_cost_whole,
        LAG(first_known_cost_whole) OVER w_grp AS prev_first_known_cost_whole,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY status ORDER BY cumulative_cost_whole, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        shipped_date_plus_59d,
        cumulative_cost_whole,
        prev_cumulative_cost_whole,
        seq_no,
        prev_first_known_cost_whole - first_known_cost_whole AS prev_first_known_cost_whole_minus_first_known_cost_whole,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM with_windows_shipments
    WHERE carrier IS NOT NULL
      AND cost_whole BETWEEN 30 AND 58
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY shipped_date_plus_59d DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    DATE_TRUNC('MONTH', shipped_date_plus_59d) AS activity_month,
    SUM(CASE WHEN carrier = 'UPS' THEN cumulative_cost_whole ELSE 0 END) AS ups_cumulative_cost_whole,
    COUNT(*) FILTER (WHERE carrier = 'DHL') AS dhl_rows
FROM latest
GROUP BY DATE_TRUNC('YEAR', delivered_date), DATE_TRUNC('MONTH', shipped_date_plus_59d)
ORDER BY dhl_rows
