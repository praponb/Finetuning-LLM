select id
from sessions
except
select id
from sessions
where to_date(started_at) < '2024-06-01'
