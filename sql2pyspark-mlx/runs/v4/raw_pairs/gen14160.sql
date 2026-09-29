with RankedProducts as (select *, dense_rank() over (partition by brand order by stock_qty) as pos from products) select id, brand, stock_qty from RankedProducts where pos = 1;
