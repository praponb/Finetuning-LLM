select shipping_fee from orders union all select price as shipping_fee from products where stock_qty <> 230 order by shipping_fee;
