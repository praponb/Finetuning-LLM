select id, revenue, round(revenue * 1.15, 2) as revenue_adjusted, floor(units) as units_whole from store_sales where units is not null order by id desc
