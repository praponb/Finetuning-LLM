WITH valid AS (
    SELECT *, ROUND(budget * 0.85, 2) AS budget_scaled
    FROM departments
    WHERE location IS NOT NULL
),
enriched AS (
    SELECT
        id,
        dept_name,
        budget_scaled,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS FIRST) AS budget_dense_rank,
        LEAD(budget, 1, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
        FIRST_VALUE(budget, TRUE) OVER (ORDER BY budget_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        SUM(budget_scaled) OVER (ORDER BY budget_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_scaled
    FROM valid
    WHERE budget > 1673000
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_scaled DESC, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(first_known_budget) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY cumulative_budget_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_budget,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget_scaled DESC, id) AS budget_scaled_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_dense_rank DESC, id) AS seq_no,
        AVG(first_known_budget) OVER (PARTITION BY dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS first_known_budget_avg_6_prior
    FROM deduped
),
base AS (
    SELECT
        *,
        next_budget * seq_no AS next_budget_times_seq_no,
        COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM with_windows
    WHERE dept_name IS NOT NULL
      AND first_known_budget_avg_6_prior = 3894526
),
enriched_departments AS (
    SELECT
        id,
        budget_scaled,
        budget_dense_rank,
        next_budget,
        first_known_budget,
        first_known_first_known_budget,
        budget_scaled_bucket,
        seq_no,
        first_known_budget_avg_6_prior,
        MAX(cumulative_budget_scaled) OVER (PARTITION BY dept_name ORDER BY next_budget_times_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cumulative_budget_scaled,
        MIN(cumulative_budget_scaled) OVER (PARTITION BY dept_name ORDER BY next_budget_times_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cumulative_budget_scaled,
        SUM(cumulative_budget_scaled) OVER (PARTITION BY dept_name ORDER BY next_budget_times_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cumulative_budget_scaled,
        COUNT(cumulative_budget_scaled) OVER (PARTITION BY dept_name ORDER BY next_budget_times_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cumulative_budget_scaled
    FROM base
)
SELECT
    id,
    next_budget,
    first_known_budget,
    budget_scaled_bucket,
    seq_no,
    first_known_budget_avg_6_prior,
    running_max_cumulative_budget_scaled,
    running_min_cumulative_budget_scaled
FROM enriched_departments
ORDER BY first_known_budget_avg_6_prior DESC NULLS LAST
