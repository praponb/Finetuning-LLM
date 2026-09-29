WITH latest_match AS (
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        x.launch_date
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY launch_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY launch_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY launch_date, id) AS row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY launch_date, id) AS row_seq_3
    FROM latest_match
),
remaining_keys AS (
    SELECT country, id
    FROM with_windows
    EXCEPT
    SELECT country, id
    FROM suppliers
    WHERE city NOT IN ('Singapore', 'London', 'Berlin')
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.id = k.id
),
not_flagged AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty > 280
        )
),
not_flagged_suppliers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty BETWEEN 154 AND 388
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= s.launch_date
        )
),
enriched AS (
    SELECT
        supplier_name,
        country,
        seq_no,
        row_seq_2,
        RANK() OVER (PARTITION BY country ORDER BY seq_no) AS seq_no_rank,
        RANK() OVER (ORDER BY row_seq) AS row_seq_rank,
        ROUND(row_seq_3 / SUM(row_seq_3) OVER (PARTITION BY country), 4) AS row_seq_3_share,
        ROUND(row_seq_3 / SUM(row_seq_3) OVER (PARTITION BY country), 4) AS row_seq_3_share_2
    FROM kept
    WHERE seq_no <> 5
)
SELECT
    CASE WHEN row_seq_3_share_2 >= 0.73 THEN 'HIGH' ELSE 'LOW' END AS row_seq_3_share_2_tier,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    AVG(row_seq_3_share) AS avg_row_seq_3_share,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows
FROM enriched
GROUP BY 1
ORDER BY th_rows
