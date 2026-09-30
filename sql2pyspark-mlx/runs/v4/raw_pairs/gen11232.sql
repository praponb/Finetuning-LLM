WITH valid AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        CASE WHEN units >= 43 THEN 'High' WHEN units >= 8 THEN 'Medium' ELSE 'Low' END AS units_tier
    FROM store_sales
    WHERE units = 37
)
SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1
