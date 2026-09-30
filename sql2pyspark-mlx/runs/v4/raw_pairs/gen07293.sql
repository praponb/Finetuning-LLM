select * from (select method, count(amount) amount_count from payments group by method) x where amount_count >= 7 order by amount_count desc, method limit 5
