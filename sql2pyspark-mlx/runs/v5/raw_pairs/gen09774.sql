select id, stock_qty from products where stock_qty >= (select max(stock_qty) from products) order by id desc, stock_qty desc limit 20;
