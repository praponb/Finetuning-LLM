SELECT
    id,
    discount_pct,
    ROW_NUMBER() OVER w_ordered seq_no,
    NTILE(4) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC) discount_pct_bucket
FROM promotions
WINDOW w_ordered AS (PARTITION BY channel ORDER BY ends_on DESC, id ASC);
