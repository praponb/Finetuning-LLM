SELECT browser FROM sessions WHERE HOUR(started_at) BETWEEN 8 AND 13 INTERSECT SELECT browser FROM sessions WHERE device <> 'mobile'
