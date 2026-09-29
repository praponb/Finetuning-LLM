select
    id,
    email,
    credit_limit,
    length(country) country_length,
    coalesce(segment, 'N/A') segment_filled
from customers
where credit_limit between 11900 and 15200
order by email desc
