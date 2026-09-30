WITH valid AS (
    SELECT
        *,
        ADD_MONTHS(end_date, 3) AS end_date_plus_months,
        ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM subscriptions
    WHERE customer_id IN (16, 18)
),
scored AS (
    SELECT
        id,
        customer_id,
        monthly_fee_rounded,
        SUM(monthly_fee_rounded) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee_rounded,
        ROW_NUMBER() OVER (ORDER BY end_date_plus_months DESC, id) AS event_seq,
        SUM(monthly_fee) OVER (PARTITION BY status) AS status_sum_monthly_fee
    FROM valid
),
base AS (
    SELECT
        id,
        customer_id,
        cumulative_monthly_fee_rounded,
        event_seq,
        status_sum_monthly_fee,
        ROUND(status_sum_monthly_fee * 1.1, 2) AS status_sum_monthly_fee_scaled
    FROM scored
    WHERE customer_id IS NOT NULL
      AND cumulative_monthly_fee_rounded <= 345
),
enriched AS (
    SELECT
        src.*,
        d.segment,
        d.signup_date,
        d.credit_limit
    FROM base src
    INNER JOIN customers d
        ON src.customer_id <=> d.id
    WHERE d.signup_date BETWEEN '2023-12-01' AND '2024-07-01'
),
joined AS (
    SELECT
        a.*,
        b.country,
        b.credit_limit AS customer_credit_limit
    FROM enriched a
    LEFT JOIN customers b
        ON a.customer_id <=> b.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY status_sum_monthly_fee_scaled DESC NULLS LAST, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
)
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN customer_credit_limit >= 13500 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_credit_limit_tier,
    COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
    APPROX_PERCENTILE(status_sum_monthly_fee_scaled, ARRAY(0.25, 0.5, 0.75)) AS status_sum_monthly_fee_scaled_quartiles,
    MAX(status_sum_monthly_fee) FILTER (WHERE segment = 'SMB') AS smb_max_status_sum_monthly_fee
FROM ranked
GROUP BY activity_month, customer_credit_limit_tier
ORDER BY smb_max_status_sum_monthly_fee;
