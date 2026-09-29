select ss.sale_date, p.price, s.sqft, s.region from store_sales ss left outer join products p on ss.product_id = p.id full outer join stores s on s.id = ss.store_id;
