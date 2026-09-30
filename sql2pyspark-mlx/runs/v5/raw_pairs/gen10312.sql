WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months
    FROM shipments
    WHERE carrier IS NOT DISTINCT FROM 'Flash'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        shipped_date_plus_months,
        LAG(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS seq_no
    FROM valid
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        shipped_date_plus_months,
        prev_cost,
        LAG(seq_no) OVER w_hist AS prev_seq_no,
        LAG(prev_cost) OVER w_hist AS prev_prev_cost,
        LEAD(prev_cost) OVER w_hist AS next_prev_cost,
        ROW_NUMBER() OVER w_hist AS seq_no_2,
        SUM(seq_no) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no
    FROM scored
    WINDOW w_hist AS (ORDER BY seq_no, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(shipped_date_plus_months, 'yyyy-MM') AS shipped_date_plus_months_label
    FROM scored_shipments
    WHERE delivered_date >= TO_DATE('2023-08-28')
)
-- Grouped metrics
SELECT
    running_seq_no,
    PERCENTILE_APPROX(prev_cost, 0.5) AS prev_cost_p50,
    APPROX_PERCENTILE(prev_seq_no, ARRAY(0.25, 0.5, 0.75)) AS prev_seq_no_quartiles
FROM cleaned
GROUP BY running_seq_no
ORDER BY prev_cost_p50 NULLS FIRST
