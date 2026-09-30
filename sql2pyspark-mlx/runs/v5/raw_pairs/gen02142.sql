select s.device, round(stddev(we.duration_sec), 2) stddev_duration_sec from web_events we left outer join sessions s on we.session_id = s.id group by s.device order by device desc;
