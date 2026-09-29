WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        status,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at >= DATE '2023-07-26'
      AND amount BETWEEN 600 AND 760
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS event_seq
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(event_seq, 1) OVER (PARTITION BY status ORDER BY event_seq, id) AS next_event_seq,
        AVG(event_seq) OVER (PARTITION BY status ORDER BY paid_at_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_seq_avg_6_prior,
        LEAD(event_seq, 1) OVER (ORDER BY paid_at, id) AS next_event_seq_2,
        RANK() OVER w_ordered AS event_seq_rank,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY event_seq DESC) AS event_seq_dense_rank
    FROM with_windows
    WHERE HOUR(paid_at) BETWEEN 11 AND 15
    WINDOW w_ordered AS (PARTITION BY status, method ORDER BY event_seq NULLS LAST)
),
with_windows_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY status, method ORDER BY next_event_seq_2, id) AS next_event_seq_2_bucket,
        COUNT(next_event_seq_2) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS next_event_seq_2_count_90d,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY event_seq_rank DESC, id) AS event_seq_2
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        paid_at,
        status,
        paid_at_date,
        event_seq,
        event_seq_avg_6_prior,
        next_event_seq_2,
        event_seq_rank,
        next_event_seq_2_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY event_seq_rank NULLS LAST, id) AS row_num
        FROM with_windows_payments
    ) t
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    status,
    DATE_TRUNC('QUARTER', paid_at_date) AS quarter_start,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(event_seq_rank, ARRAY(0.25, 0.5, 0.75)) AS event_seq_rank_quartiles,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS event_seq_per_failed,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) DESC) AS event_seq_per_failed_percent_rank
FROM deduped
GROUP BY status, DATE_TRUNC('QUARTER', paid_at_date)
HAVING event_seq_per_failed > 0;
