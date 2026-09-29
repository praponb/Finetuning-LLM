select * from (select id, carrier, cost, row_number() over (partition by carrier order by cost desc, id) rnk from shipments) ranked where rnk <= 2 order by cost desc
