select
    id,
    city,
    opened_date,
    sqft,
    row_number() over (partition by city order by opened_date, id) as opened_date_row,
    lead(sqft, 2, 0) over (partition by city order by opened_date, id) as next_sqft
from stores
order by city, sqft desc;
