select id, created_at, items, attributes, array_max(transform(items, li -> li.price)) as max_item_price, attributes['gift'] as gift from carts
