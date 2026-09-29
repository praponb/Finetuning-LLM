select
    *,
    lead(rating, 2) over (partition by city order by rating asc, id asc) as next_rating,
    sum(rating) over (partition by country order by rating asc, id asc rows between unbounded preceding and current row) as running_rating,
    row_number() over (partition by city order by rating desc, id asc) as event_seq,
    avg(rating) over (partition by country order by rating asc, id asc rows between 3 preceding and current row) as rating_avg_3,
    sum(rating) over (order by rating asc, id asc rows between unbounded preceding and current row) as cumulative_rating
from suppliers
