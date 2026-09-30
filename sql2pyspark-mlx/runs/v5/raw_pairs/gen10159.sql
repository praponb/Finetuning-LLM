select case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort, sum(units) as total_units, round(sum(units) * 100.0 / sum(sum(units)) over (), 2) as pct_of_total
from store_sales
where revenue <= 1900
group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end;
