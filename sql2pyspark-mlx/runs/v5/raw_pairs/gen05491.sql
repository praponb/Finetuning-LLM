WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        cost,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
        LOWER(status) AS status_lower,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE carrier IN ('UPS', 'Flash', 'Kerry')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY days_since_delivered_date DESC, id) AS event_seq,
        COUNT(days_since_delivered_date) OVER (PARTITION BY order_id) AS order_id_count_days_since_delivered_date
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        cost,
        days_since_delivered_date,
        status_lower,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY order_id_count_days_since_delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        LEAD(order_id_count_days_since_delivered_date, 2) OVER (PARTITION BY carrier ORDER BY event_seq, id) AS next_order_id_count_days_since_delivered_date,
        FIRST_VALUE(days_since_delivered_date, TRUE) OVER (ORDER BY order_id_count_days_since_delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_delivered_date
    FROM enriched
),
cleaned_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        days_since_delivered_date + first_known_days_since_delivered_date AS days_since_delivered_date_plus_first_known_days_since_delivered_date,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM metrics
    WHERE carrier IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        cost,
        days_since_delivered_date,
        status_lower,
        cumulative_cost,
        next_order_id_count_days_since_delivered_date,
        first_known_days_since_delivered_date,
        carrier_clean,
        FLOOR(next_order_id_count_days_since_delivered_date) AS next_order_id_count_days_since_delivered_date_whole,
        FLOOR(days_since_delivered_date_plus_first_known_days_since_delivered_date) AS days_since_delivered_date_plus_first_known_days_since_delivered_date_whole
    FROM cleaned_shipments
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'DHL'
),
cleaned_shipments_2 AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        days_since_delivered_date_plus_first_known_days_since_delivered_date_whole + next_order_id_count_days_since_delivered_date AS days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date,
        LEAST(days_since_delivered_date_plus_first_known_days_since_delivered_date_whole, next_order_id_count_days_since_delivered_date_whole) AS least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole
    FROM valid
    WHERE first_known_days_since_delivered_date >= 47
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(cost) IGNORE NULLS OVER (ORDER BY next_order_id_count_days_since_delivered_date_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        AVG(cost) OVER (ORDER BY next_order_id_count_days_since_delivered_date_whole, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM cleaned_shipments_2
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date,
        last_known_cost,
        least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole
    FROM enriched_shipments
) src
UNPIVOT (
    metric_value FOR metric_name IN (days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_plus_next_order_id_count_days_since_delivered_date, last_known_cost, least_days_since_delivered_date_plus_first_known_days_since_delivered_date_whole_next_order_id_count_days_since_delivered_date_whole)
) unp
