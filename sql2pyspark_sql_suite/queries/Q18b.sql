-- Q18b: Session windows with a dynamic gap (checkout closes a session faster)
-- needs setup views: web_events
-- @id: Q18b
SELECT
  customer_id,
  session_window.start                                              AS session_start,
  session_window.end                                                AS session_end,
  timestampdiff(MINUTE, session_window.start, session_window.end)  AS window_minutes,
  COUNT(*)                                                          AS events,
  max(event_type = 'checkout')                                      AS converted,
  min_by(event_type, event_ts)                                      AS first_event,
  max_by(event_type, event_ts)                                      AS last_event,
  date_format(from_utc_timestamp(session_window.start, 'Asia/Bangkok'), 'yyyy-MM-dd HH:mm') AS start_bkk
FROM web_events
WHERE customer_id IS NOT NULL
GROUP BY customer_id,
         session_window(event_ts, CASE WHEN event_type = 'checkout' THEN '5 minutes' ELSE '20 minutes' END)
ORDER BY customer_id, session_start;
