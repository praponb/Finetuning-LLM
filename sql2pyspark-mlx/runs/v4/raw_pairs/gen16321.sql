select
    *,
    sum(rating) over (partition by country, city order by rating, id rows between unbounded preceding and current row) as cumulative_rating,
    row_number() over (order by rating desc, id) as seq_no,
    row_number() over (order by rating, id) as event_seq
from suppliers
