WITH base AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total DESC, id ASC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
base_order_items AS (
    SELECT
        id,
        unit_price,
        LEAST(rank_in_group, line_total) AS least_rank_in_group_line_total
    FROM deduped
    WHERE product_id IS NOT NULL
),
filtered AS (
    SELECT *, CEIL(least_rank_in_group_line_total) AS least_rank_in_group_line_total_whole
    FROM base_order_items
    WHERE least_rank_in_group_line_total > 78
),
scored AS (
    SELECT
        id,
        unit_price,
        least_rank_in_group_line_total,
        least_rank_in_group_line_total_whole,
        CUME_DIST() OVER (ORDER BY least_rank_in_group_line_total ASC NULLS FIRST) AS least_rank_in_group_line_total_cume_dist,
        ROW_NUMBER() OVER w_grp AS event_seq,
        AVG(least_rank_in_group_line_total) OVER (ORDER BY least_rank_in_group_line_total_whole ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS least_rank_in_group_line_total_avg_5
    FROM filtered
    WINDOW w_grp AS (ORDER BY unit_price DESC, id ASC)
)
SELECT
    id,
    unit_price,
    least_rank_in_group_line_total_cume_dist
FROM scored
ORDER BY least_rank_in_group_line_total_cume_dist DESC NULLS LAST;
