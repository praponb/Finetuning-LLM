WITH cleaned AS (
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CAST(is_active AS INT) AS is_active_flag,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM customers
    WHERE signup_date >= TO_DATE('2023-07-16')
      AND city IS DISTINCT FROM 'Singapore'
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY is_active_flag DESC, id) AS row_seq,
        SUM(is_active_flag) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_active_flag,
        LAST_VALUE(age, TRUE) OVER w_grp AS last_known_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit, id) AS seq_no
    FROM cleaned
    WHERE country = 'UK'
    WINDOW w_grp AS (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    SELECT
        s.*,
        x.device,
        x.session_country
    FROM enriched s
    JOIN LATERAL (
            SELECT c.device, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        is_active_flag,
        segment_clean,
        row_seq,
        running_is_active_flag,
        last_known_age,
        seq_no,
        device,
        session_country,
        seq_no * row_seq AS seq_no_times_row_seq
    FROM with_last
    WHERE signup_date >= DATE '2023-04-03'
      AND session_country IS NOT NULL
),
daily AS (
    SELECT
        CASE WHEN is_active_flag >= 83 THEN 'HIGH' ELSE 'LOW' END AS is_active_flag_tier,
        segment,
        BIT_OR(age) AS age_bit_or,
        ROUND(SUM(last_known_age) / NULLIF(COUNT(CASE WHEN session_country = 'DE' THEN 1 END), 0), 2) AS last_known_age_per_de,
        COUNT(*) AS cnt,
        COUNT_IF(device = 'desktop') AS desktop_events,
        COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN is_active_flag >= 83 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(SUM(last_known_age) / NULLIF(COUNT(CASE WHEN session_country = 'DE' THEN 1 END), 0), 2) DESC) AS last_known_age_per_de_percent_rank
    FROM valid
    GROUP BY CASE WHEN is_active_flag >= 83 THEN 'HIGH' ELSE 'LOW' END, segment
    HAVING ROUND(SUM(last_known_age) / NULLIF(COUNT(CASE WHEN session_country = 'DE' THEN 1 END), 0), 2) >= 0
)
SELECT
    is_active_flag_tier,
    segment,
    last_known_age_per_de,
    desktop_events,
    last_known_age_per_de_percent_rank
FROM daily
ORDER BY segment
