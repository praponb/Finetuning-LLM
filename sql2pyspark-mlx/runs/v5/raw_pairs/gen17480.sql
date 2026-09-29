select country, sum(rating) as total_rating, round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
where country in ('DE', 'TH')
group by country
order by country desc, total_rating desc;
