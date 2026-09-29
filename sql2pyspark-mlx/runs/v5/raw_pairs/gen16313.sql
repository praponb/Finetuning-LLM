WITH valid AS (
    SELECT
        id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CASE WHEN channel = 'partner' THEN 1 ELSE 0 END is_partner,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM orders
    WHERE store_id IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY is_partner DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY channel, status ORDER BY dedup_rank ASC) AS dedup_rank_2
    FROM ranked
    QUALIFY dedup_rank_2 = 1
),
metrics AS (
    SELECT
        id,
        status,
        ROW_NUMBER() OVER w_running row_seq,
        NTILE(4) OVER (PARTITION BY status ORDER BY amount ASC, id ASC) amount_bucket
    FROM deduped
    WINDOW w_running AS (PARTITION BY status ORDER BY discount DESC, id ASC)
),
kept_keys AS (
    SELECT id, status
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, status
    FROM orders
    WHERE shipping_fee >= 20
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
enriched AS (
    SELECT
        id,
        row_seq,
        amount_bucket,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY row_seq ASC NULLS FIRST) row_seq_dense_rank,
        ROUND(amount_bucket / SUM(amount_bucket) OVER (PARTITION BY status), 4) amount_bucket_share,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount_bucket DESC NULLS LAST) amount_bucket_percent_rank
    FROM verified
),
enriched_orders AS (
    SELECT
        row_seq,
        amount_bucket,
        row_seq_dense_rank,
        amount_bucket_share,
        amount_bucket_percent_rank,
        RANK() OVER (ORDER BY amount_bucket_share ASC NULLS FIRST) amount_bucket_share_rank,
        CUME_DIST() OVER (ORDER BY amount_bucket_percent_rank DESC NULLS LAST) amount_bucket_percent_rank_cume_dist,
        RANK() OVER (ORDER BY amount_bucket_share DESC NULLS LAST) amount_bucket_share_rank_2
    FROM enriched
)
SELECT
    CASE WHEN amount_bucket_share_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS amount_bucket_share_rank_2_tier,
    CASE WHEN row_seq_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_dense_rank_tier,
    AVG(amount_bucket_share_rank_2) AS avg_amount_bucket_share_rank_2,
    ROUND(AVG(amount_bucket_share), 2) AS avg_amount_bucket_share,
    SUM(amount_bucket_percent_rank) AS total_amount_bucket_percent_rank,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    MAX(amount_bucket_share_rank) AS max_amount_bucket_share_rank,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN amount_bucket_share_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(amount_bucket_share), 2) DESC) AS avg_amount_bucket_share_percent_rank
FROM enriched_orders
GROUP BY 1, 2;
