select event_type, event_time, duration_sec, datediff(current_date(), event_time) days_since_event_time from web_events where duration_sec is not null;
