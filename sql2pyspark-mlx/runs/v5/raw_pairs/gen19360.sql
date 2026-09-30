SELECT id, store_id, product_id, revenue * units AS revenue_per_unit FROM store_sales WHERE units != 38 ORDER BY id DESC, store_id DESC LIMIT 20
