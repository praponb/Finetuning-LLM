WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(store_name) AS store_name_upper
    FROM stores
    WHERE opened_date > DATE '2023-10-04'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
    FROM valid
    WHERE region IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS seq_no,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(cumulative_sqft) OVER w_ordered AS next_cumulative_sqft,
        LEAD(sqft) OVER w_ordered AS next_sqft_2,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY region, city ORDER BY seq_no, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, region
    FROM with_windows
    EXCEPT
    SELECT city, region
    FROM stores
    WHERE opened_date > '2024-05-18'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.city = k.city AND s.region = k.region
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY region), 4) AS seq_no_share,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY seq_no DESC NULLS FIRST) AS seq_no_dense_rank,
        DENSE_RANK() OVER (ORDER BY next_sqft NULLS FIRST) AS next_sqft_dense_rank
    FROM kept_rows
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    store_name_upper,
    opened_date_label,
    seq_no,
    CASE WHEN store_name RLIKE '^(Riverside|Siam Flagship)$' THEN 'GROUP_A' WHEN store_name RLIKE '^(Downtown Express|Airport Kiosk)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS store_name_zone
FROM with_windows_stores
ORDER BY id DESC, opened_date;
