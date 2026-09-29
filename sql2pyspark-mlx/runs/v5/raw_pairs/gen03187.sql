WITH scored AS (
    SELECT
        id,
        carrier,
        shipped_date,
        MIN(cost) OVER (PARTITION BY carrier, status ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_cost,
        SUM(cost) OVER (PARTITION BY carrier, status ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_cost,
        MAX(cost) OVER (PARTITION BY carrier, status ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_cost,
        COUNT(cost) OVER (PARTITION BY carrier, status ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_cost
    FROM shipments
    WHERE delivered_date > '2023-03-01'
),
scored_shipments AS (
    SELECT
        id,
        carrier,
        shipped_date,
        last3_sum_cost,
        last3_max_cost,
        DENSE_RANK() OVER (ORDER BY last3_count_cost DESC) last3_count_cost_dense_rank,
        ROUND(last3_min_cost / SUM(last3_min_cost) OVER (PARTITION BY carrier), 4) last3_min_cost_share,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY last3_sum_cost DESC NULLS FIRST) last3_sum_cost_dense_rank
    FROM scored
),
enriched AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY last3_sum_cost DESC, id) last3_sum_cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY last3_max_cost DESC, id) event_seq,
        SUM(last3_min_cost_share) OVER (PARTITION BY carrier ORDER BY last3_count_cost_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_last3_min_cost_share
    FROM scored_shipments
),
deduped AS (
    SELECT
        id,
        carrier,
        shipped_date,
        last3_sum_cost,
        last3_max_cost,
        last3_min_cost_share,
        last3_sum_cost_dense_rank,
        event_seq
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY last3_sum_cost_dense_rank DESC NULLS LAST, id) = 1
),
summary AS (
    SELECT
        DATE_TRUNC('WEEK', shipped_date) AS week_start,
        MAX(last3_sum_cost) AS max_last3_sum_cost,
        SUM(last3_sum_cost_dense_rank) FILTER (WHERE carrier = 'Kerry') AS kerry_total_last3_sum_cost_dense_rank,
        ROUND(AVG(last3_sum_cost_dense_rank), 2) AS avg_last3_sum_cost_dense_rank,
        ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN carrier = 'DHL' THEN 1 END), 0), 2) AS event_seq_per_dhl,
        PERCENTILE_APPROX(last3_sum_cost, 0.9) AS last3_sum_cost_p90
    FROM deduped
    GROUP BY DATE_TRUNC('WEEK', shipped_date)
    HAVING PERCENTILE_APPROX(last3_sum_cost, 0.9) > 0
)
SELECT
    week_start,
    max_last3_sum_cost,
    kerry_total_last3_sum_cost_dense_rank,
    avg_last3_sum_cost_dense_rank,
    last3_sum_cost_p90
FROM summary
