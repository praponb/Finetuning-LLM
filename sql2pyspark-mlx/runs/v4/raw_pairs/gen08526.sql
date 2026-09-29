WITH enriched AS (SELECT a.*, b.device, b.browser FROM web_events a LEFT JOIN sessions b ON a.session_id = b.id) SELECT id, event_type, page, browser FROM enriched
