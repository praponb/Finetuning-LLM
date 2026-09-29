select id, location
from departments
except distinct
select id, location
from departments
where budget > 1337000;
