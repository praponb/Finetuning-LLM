select id, city, rating, country from (select *, row_number() over (partition by city order by rating desc, id) as rnk from suppliers) as t where rnk <= 2;
