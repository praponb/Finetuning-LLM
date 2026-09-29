SELECT ss.sale_date, ss.units, s.city, s.region FROM store_sales ss JOIN stores s ON ss.store_id = s.id WHERE ss.revenue < 2200;
