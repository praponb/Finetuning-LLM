SELECT
    s.browser,
    s.country,
    COUNT(*) AS cnt,
    MAX(src.duration_sec) AS max_duration_sec,
    GROUPING_ID() AS level_id
FROM web_events src
INNER JOIN sessions s ON src.session_id = s.id
GROUP BY GROUPING SETS ((s.browser, s.country), (s.browser), ())
ORDER BY cnt DESC NULLS LAST;
