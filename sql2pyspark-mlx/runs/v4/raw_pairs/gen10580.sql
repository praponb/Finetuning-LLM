select city, 'customer' as source from customers union all select city, 'store' as source from stores order by city;
