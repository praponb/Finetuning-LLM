WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant <> 'Amazon'
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        YEAR(opened_date) AS opened_date_year,
        COALESCE(status, 'N/A') AS status_clean
    FROM not_flagged
    WHERE opened_date > DATE '2023-04-01'
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, d.country
    FROM base s
    JOIN customers d
        ON s.customer_id = d.id
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER (PARTITION BY customer_id ORDER BY opened_date_year NULLS FIRST) AS opened_date_year_rank,
    LAST_VALUE(opened_date_year) IGNORE NULLS OVER (PARTITION BY status, country ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_date_year
FROM joined;
