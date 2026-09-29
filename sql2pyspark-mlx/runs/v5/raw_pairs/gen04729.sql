WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k, customer_id ORDER BY created_at DESC, id ASC, k ASC) AS dedup_rank
        FROM lines
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id ASC, k ASC) AS dedup_rank_2
    FROM deduped
    QUALIFY dedup_rank_2 = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        coupon_codes,
        k,
        v,
        dedup_rank_2,
        ROW_NUMBER() OVER (ORDER BY dedup_rank_2 ASC, id ASC, k ASC) AS seq_no
    FROM ranked
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY dedup_rank_2 DESC, id ASC, k ASC) AS event_seq,
        NTILE(3) OVER (PARTITION BY k ORDER BY seq_no DESC, id ASC, k ASC) AS seq_no_bucket
    FROM scored
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) < '2025-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_bucket_tier,
    seq_no,
    CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT(DISTINCT k) AS distinct_ks_2,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50,
    PERCENTILE_APPROX(event_seq, 0.9) AS event_seq_p90,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN seq_no_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END ORDER BY PERCENTILE_APPROX(event_seq, 0.5) DESC) AS event_seq_p50_percent_rank
FROM reconciled
GROUP BY CASE WHEN seq_no_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END, seq_no, CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
