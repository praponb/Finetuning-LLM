WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(status) AS status_upper
    FROM shipments
    WHERE carrier IN ('Kerry', 'Flash')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_bucket,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) AS row_seq
    FROM valid
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_upper,
        cost_bucket,
        row_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share_2,
        SUM(cost_share) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_share,
        AVG(row_seq) OVER (PARTITION BY order_id ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_seq_avg_3_prior,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY status), 4) AS row_seq_share,
        LEAD(cost, 2, 0) OVER (PARTITION BY carrier, status ORDER BY row_seq, id) AS next_cost
    FROM with_windows
    WHERE shipped_date BETWEEN '2024-01-01' AND '2024-02-01'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        delivered_date,
        cost,
        cost_bucket,
        row_seq,
        cost_share_2,
        cumulative_cost_share,
        row_seq_avg_3_prior,
        row_seq_share,
        LEAST(cost, cost_bucket) AS least_cost_cost_bucket
    FROM scored
    WHERE shipped_date > DATE '2023-07-28'
)
SELECT
    id,
    cost_bucket,
    cost_share_2,
    cumulative_cost_share,
    row_seq_avg_3_prior
FROM cleaned;
