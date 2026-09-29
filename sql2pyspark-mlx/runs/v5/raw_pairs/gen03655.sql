WITH scored AS (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER w_ordered event_seq,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share_2
    FROM departments
    WHERE budget > 3950000
    WINDOW w_ordered AS (PARTITION BY dept_name, location ORDER BY budget DESC, id)
),
enriched AS (
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY dept_name ORDER BY budget_share, id) prev_budget,
        LEAD(budget_share_2) OVER (PARTITION BY dept_name ORDER BY budget_share, id) next_budget_share_2,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_share, id) seq_no
    FROM scored
),
metrics AS (
    SELECT
        id,
        event_seq,
        budget_share,
        budget_share_2,
        next_budget_share_2,
        LAG(next_budget_share_2, 2) OVER (ORDER BY event_seq, id) prev_next_budget_share_2,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY dept_name), 4) seq_no_share
    FROM enriched
),
scored_departments AS (
    SELECT
        *,
        LEAD(prev_next_budget_share_2) OVER w_grp next_prev_next_budget_share_2,
        LAG(budget_share) OVER w_grp prev_budget_share,
        ROW_NUMBER() OVER w_grp event_seq_2
    FROM metrics
    WINDOW w_grp AS (ORDER BY seq_no_share, id)
)
SELECT
    CASE WHEN next_prev_next_budget_share_2 >= 0.88 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_prev_next_budget_share_2_tier,
    APPROX_PERCENTILE(budget_share, 0.9) AS budget_share_p90,
    ROUND(AVG(prev_next_budget_share_2), 2) AS avg_prev_next_budget_share_2,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(prev_next_budget_share_2, ARRAY(0.25, 0.5, 0.75)) AS prev_next_budget_share_2_quartiles
FROM scored_departments
GROUP BY ROLLUP (CASE WHEN next_prev_next_budget_share_2 >= 0.88 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING ROUND(AVG(prev_next_budget_share_2), 2) > 0;
