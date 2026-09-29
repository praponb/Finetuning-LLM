select id, country from suppliers where country = 'SG' union select id, country from suppliers where country not in ('SG', 'DE') order by id desc
