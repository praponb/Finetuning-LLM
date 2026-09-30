select s.*
from suppliers s
where not exists (
        select 1
        from products ch
        where ch.supplier_id = s.id
          and ch.cost > 610
    );
