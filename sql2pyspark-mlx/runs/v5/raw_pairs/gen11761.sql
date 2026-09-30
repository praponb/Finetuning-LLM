WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(event_type, ', ', page) event_type_page
    FROM web_events
    WHERE page IS DISTINCT FROM '/product'
      AND TO_DATE(event_time) < '2023-03-09'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) row_seq,
        FIRST_VALUE(page, TRUE) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_page,
        ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY event_time, id) event_seq,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_page,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) seq_no
    FROM deduped
    WHERE page IN ('/search', '/checkout')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(row_seq, TRUE) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_row_seq,
        AVG(seq_no) OVER (ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) seq_no_avg_3
    FROM enriched
)
SELECT
    id,
    session_id,
    row_seq,
    first_known_page,
    event_seq,
    seq_no,
    first_known_row_seq,
    seq_no_avg_3
FROM scored
ORDER BY seq_no NULLS FIRST;
