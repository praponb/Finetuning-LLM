select *, substring(city, 1, 3) as city_prefix from customers where credit_limit < 7200 order by 3 asc;
