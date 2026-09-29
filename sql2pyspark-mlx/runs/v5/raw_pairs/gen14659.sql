select price from products where stock_qty >= 335 except select unit_price from order_items order by price;
