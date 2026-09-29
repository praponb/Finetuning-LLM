select *, row_number() over (partition by city order by rating desc nulls last, id) as latest_rank from suppliers qualify latest_rank = 1;
