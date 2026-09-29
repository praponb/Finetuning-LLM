select src.id, src.customer_id, src.coupon_codes, li_item.qty, li_item.price, li_item.promo from carts src lateral view outer explode(src.items) li as li_item;
