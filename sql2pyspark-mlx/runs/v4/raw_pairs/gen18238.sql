SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS event_seq,
    CUME_DIST() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist
FROM stores
