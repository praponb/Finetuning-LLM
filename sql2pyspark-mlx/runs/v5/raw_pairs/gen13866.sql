select id, started_at, initcap(trim(country)) as country_title
from sessions
order by started_at asc
