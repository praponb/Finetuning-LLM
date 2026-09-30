WITH filtered AS (
    SELECT
        id,
        customer_id,
        device,
        LOWER(country) AS country_lower,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE country IS NOT NULL
),
enriched AS (
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.country_lower,
        c.segment
    FROM filtered e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND TO_DATE(x.event_time) < '2025-03-16'
        )
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.segment,
        p.age,
        p.email
    FROM kept s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
    WHERE p.email IS NOT NULL
),
enriched_sessions AS (
    SELECT
        id,
        customer_id,
        segment,
        age,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY age, id) AS age_bucket,
        ROW_NUMBER() OVER (ORDER BY age DESC, id) AS row_seq
    FROM joined
),
summary AS (
    SELECT
        CASE WHEN age_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS age_bucket_tier,
        segment,
        COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
        AVG(age) AS avg_age,
        MAX(age) FILTER (WHERE segment = 'SMB') AS smb_max_age
    FROM enriched_sessions
    GROUP BY 1, 2
    HAVING corporate_rows >= 1
)
SELECT
    age_bucket_tier,
    segment,
    avg_age,
    smb_max_age
FROM summary;
