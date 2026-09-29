SELECT orders.order_date, payments.paid_at, payments.status FROM orders JOIN payments ON orders.id = payments.order_id WHERE orders.order_date >= '2024-07-01' AND payments.paid_at >= '2025-02-01'
