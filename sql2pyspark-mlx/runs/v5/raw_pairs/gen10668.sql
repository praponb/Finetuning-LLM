select
    id,
    shipped_date,
    delivered_date,
    datediff(current_date(), shipped_date) as days_since_shipped_date
from shipments
where year(delivered_date) = 2024
order by delivered_date, id
limit 10
