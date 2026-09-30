SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY unit_price DESC NULLS LAST) unit_price_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) seq_no
FROM order_items
