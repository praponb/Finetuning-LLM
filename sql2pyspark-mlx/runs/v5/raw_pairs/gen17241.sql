WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER w_recent AS seq_no,
        AVG(budget) OVER w_grp AS budget_avg_2,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS seq_no_2
    FROM departments
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget DESC, id ASC), w_grp AS (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        seq_no,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget_avg_2 ASC NULLS LAST) AS budget_avg_2_percent_rank,
        PERCENT_RANK() OVER (ORDER BY budget DESC NULLS FIRST) AS budget_percent_rank,
        AVG(seq_no_2) OVER w_grp AS seq_no_2_avg_6
    FROM metrics
    WINDOW w_grp AS (PARTITION BY dept_name ORDER BY seq_no ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
metrics_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        seq_no,
        budget_percent_rank,
        seq_no_2_avg_6,
        LEAD(seq_no) OVER w_running AS next_seq_no,
        LAG(seq_no) OVER w_running AS prev_seq_no,
        LEAD(seq_no_2_avg_6) OVER w_running AS next_seq_no_2_avg_6,
        LAG(seq_no_2_avg_6) OVER w_running AS prev_seq_no_2_avg_6,
        ROW_NUMBER() OVER w_running AS event_seq,
        NTILE(3) OVER w_hist AS budget_avg_2_percent_rank_bucket
    FROM scored
    WHERE budget_avg_2_percent_rank > 0.36
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY budget_percent_rank ASC, id ASC), w_hist AS (PARTITION BY dept_name ORDER BY budget_avg_2_percent_rank DESC, id ASC)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics_departments
    INTERSECT
    SELECT id
    FROM departments
    WHERE dept_name = 'HR'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_departments s
    JOIN kept_keys k ON s.id = k.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_name
    FROM verified
    INTERSECT
    SELECT dept_name
    FROM departments
    WHERE budget BETWEEN 2427000 AND 2836000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN remaining_keys k ON s.dept_name = k.dept_name
)
-- Grouped metrics
SELECT
    prev_seq_no,
    PERCENTILE_APPROX(next_seq_no, ARRAY(0.25, 0.5, 0.75)) AS next_seq_no_quartiles,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count
FROM reconciled
GROUP BY prev_seq_no
HAVING sales_count >= 1
