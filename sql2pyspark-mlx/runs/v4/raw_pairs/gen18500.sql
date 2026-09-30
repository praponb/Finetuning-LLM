WITH with_dims AS (
    SELECT
        src.*,
        d.region,
        d.opened_date,
        d.store_name
    FROM inventory src
    LEFT JOIN stores d
        ON src.store_id = d.id
)
SELECT
    region,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    BIT_AND(quantity) AS quantity_bit_and,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY BIT_AND(quantity) DESC) AS quantity_bit_and_percent_rank
FROM with_dims
GROUP BY region, DATE_TRUNC('QUARTER', opened_date)
HAVING quantity_bit_and < 1
ORDER BY region DESC NULLS FIRST;
