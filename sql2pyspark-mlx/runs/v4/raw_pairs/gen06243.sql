SELECT id, customer_id, items, attributes['utm_source'] AS utm_source, SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines FROM carts
