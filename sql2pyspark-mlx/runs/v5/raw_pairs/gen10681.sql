select s.*
from sessions s
where exists (
        select 1
        from web_events x
        where x.session_id = s.id
          and x.duration_sec > 280
    )
