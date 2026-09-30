SELECT id, ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text, attributes['gift'] AS gift FROM carts;
