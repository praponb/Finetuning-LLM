WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        b.city,
        b.signup_date,
        b.segment
    FROM eligible a
    INNER JOIN customers b
        ON a.customer_id = b.id
    WHERE b.segment NOT IN ('Corporate', 'SMB')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY signup_date DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date DESC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id, segment ORDER BY signup_date DESC, id ASC) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date ASC, id ASC) AS row_seq
    FROM enriched
),
with_windows_sessions AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        signup_date,
        segment,
        row_seq,
        AVG(row_seq) OVER (PARTITION BY segment ORDER BY seq_no_2 ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_seq_avg_5,
        STDDEV_SAMP(event_seq) OVER (PARTITION BY city) AS city_stddev_samp_event_seq
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        signup_date,
        row_seq_avg_5,
        city_stddev_samp_event_seq,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY segment), 4) AS row_seq_share,
        CUME_DIST() OVER w_recent AS city_stddev_samp_event_seq_cume_dist,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY city, segment), 4) AS row_seq_share_2,
        SUM(city_stddev_samp_event_seq) OVER (PARTITION BY city, segment ORDER BY city_stddev_samp_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_city_stddev_samp_event_seq
    FROM with_windows_sessions
    WINDOW w_recent AS (PARTITION BY city ORDER BY city_stddev_samp_event_seq ASC NULLS FIRST)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        city,
        CASE WHEN city_stddev_samp_event_seq_cume_dist >= 0.82 THEN 'HIGH' ELSE 'LOW' END AS city_stddev_samp_event_seq_cume_dist_tier,
        ROUND(SUM(city_stddev_samp_event_seq_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS city_stddev_samp_event_seq_cume_dist_per_chiang_mai,
        COUNT_IF(city = 'Bangkok') AS bangkok_events,
        AVG(row_seq_share) AS avg_row_seq_share,
        MIN(city_stddev_samp_event_seq) AS min_city_stddev_samp_event_seq,
        APPROX_PERCENTILE(city_stddev_samp_event_seq, 0.9) AS city_stddev_samp_event_seq_p90
    FROM metrics
    GROUP BY city, CASE WHEN city_stddev_samp_event_seq_cume_dist >= 0.82 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    city,
    city_stddev_samp_event_seq_cume_dist_tier,
    bangkok_events,
    min_city_stddev_samp_event_seq,
    CASE WHEN city RLIKE '^(New York|Tokyo)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM grouped
ORDER BY bangkok_events DESC, city_zone DESC;
