WITH valid AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        UPPER(city) AS city_upper,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-09-03'
      AND region IS DISTINCT FROM 'South'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_month_end DESC NULLS LAST, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
ranked_stores AS (
    SELECT
        id,
        store_name,
        city_upper,
        opened_date_month_end
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date_month_end DESC NULLS LAST) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
with_last AS (
    SELECT
        s.id,
        s.store_name,
        x.sale_date
    FROM ranked_stores s
    JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    SELECT s.*, top1.discount
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    sale_date,
    discount,
    LAG(discount) OVER (ORDER BY discount, id) AS prev_discount,
    LEAD(discount) OVER (ORDER BY discount, id) AS next_discount,
    ROW_NUMBER() OVER (ORDER BY discount, id) AS row_seq
FROM latest_match;
