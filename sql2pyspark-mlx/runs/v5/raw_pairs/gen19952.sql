select
    id,
    country,
    supplier_name,
    round(rating, 0) rating_rounded,
    initcap(trim(country)) country_title
from suppliers
where country = 'UK'
order by country;
