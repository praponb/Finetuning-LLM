select brand, category, round((100 * sum(case when brand = 'Stark' then 1 else 0 end)) / count(*), 1) as pct_stark from products group by brand, category
