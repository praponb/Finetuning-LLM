WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        items,
        attributes,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        ship_to.zip AS ship_zip
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    created_at,
    utm_source_or_default,
    FIRST_VALUE(utm_source_or_default) IGNORE NULLS OVER (ORDER BY created_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source_or_default,
    FIRST_VALUE(utm_source_or_default) IGNORE NULLS OVER (ORDER BY created_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source_or_default_2
FROM flattened_attrs
