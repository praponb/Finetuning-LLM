select unit_price as value from order_items intersect select monthly_fee as value from subscriptions;
