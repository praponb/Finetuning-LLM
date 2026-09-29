WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE units >= 35
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS FIRST, id) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1
