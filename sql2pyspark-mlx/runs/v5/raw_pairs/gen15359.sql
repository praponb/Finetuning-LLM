select * from suppliers qualify row_number() over (partition by city, country order by rating desc nulls first, id) = 1;
