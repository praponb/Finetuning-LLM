SELECT
    id,
    created_at,
    items,
    ship_to,
    FORALL(items, item -> item.qty <= 5) AS all_small_qty,
    SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines
FROM carts;
