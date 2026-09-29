SELECT stock_qty, cost, DATEDIFF(CURRENT_DATE(), launch_date) days_since_launch_date FROM products WHERE ARRAY_CONTAINS(tags, 'new') AND category IN ('Fashion', 'Electronics', 'Grocery');
