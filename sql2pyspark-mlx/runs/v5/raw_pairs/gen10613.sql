select * from suppliers where supplier_name is not null or country not in ('JP', 'UK') order by supplier_name desc
