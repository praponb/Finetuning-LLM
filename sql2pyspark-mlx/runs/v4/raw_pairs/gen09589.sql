select *, weekofyear(opened_date) as opened_date_weekofyear from stores where region is null and city in ('Tokyo', 'Berlin') order by region, id limit 5
