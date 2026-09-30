select browser, min(started_at) as first_seen, (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as span_minutes from sessions group by browser order by span_minutes
