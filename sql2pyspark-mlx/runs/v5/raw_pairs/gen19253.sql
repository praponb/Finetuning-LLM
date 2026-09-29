select *, row_number() over (partition by country order by rating desc nulls first, id) as rn from suppliers qualify rn = 1;
