WITH base AS (
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        DATE_ADD(shipped_date, 27) AS shipped_date_plus_27d,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-05-01')
      AND carrier IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        ROUND(cost, 0) AS cost_rounded,
        LAST_DAY(shipped_date_plus_27d) AS shipped_date_plus_27d_month_end
    FROM base
    WHERE MONTH(shipped_date_plus_27d) IN (4, 7, 8)
),
valid AS (
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        resolved_carrier,
        cost_rounded,
        shipped_date_plus_27d_month_end,
        ADD_MONTHS(shipped_date_plus_27d_month_end, 12) AS shipped_date_plus_27d_month_end_plus_months,
        ADD_MONTHS(shipped_date_plus_27d_month_end, 12) AS shipped_date_plus_27d_month_end_plus_months_2,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM cleaned
    WHERE cost BETWEEN 39 AND 71
),
scored AS (
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY status) AS status_avg_cost,
        NTILE(10) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket,
        LAG(cost_rounded, 2, 0) OVER (PARTITION BY status, carrier ORDER BY shipped_date_plus_27d_month_end_plus_months_2, id) AS prev_cost_rounded
    FROM valid
)
SELECT
    id,
    carrier,
    shipped_date,
    cost,
    status,
    shipped_date_plus_27d_month_end,
    shipped_date_plus_27d_month_end_plus_months,
    shipped_date_plus_27d_month_end_plus_months_2,
    carrier_filled,
    CASE WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(DELIVERED|LOST)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS status_zone
FROM scored
ORDER BY carrier DESC;
