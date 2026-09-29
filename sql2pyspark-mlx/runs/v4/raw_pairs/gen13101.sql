select merchant
from transactions
where amount >= 1200
union
select merchant
from transactions
where amount < 2600
order by merchant;
