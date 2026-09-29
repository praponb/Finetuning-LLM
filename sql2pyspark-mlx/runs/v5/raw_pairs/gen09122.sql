select shipping_fee from orders intersect select revenue as shipping_fee from store_sales;
