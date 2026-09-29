SELECT s.browser, SORT_ARRAY(COLLECT_SET(we.event_type)) event_type_list FROM web_events we JOIN sessions s ON we.session_id = s.id LEFT JOIN customers c ON s.customer_id = c.id GROUP BY s.browser;
