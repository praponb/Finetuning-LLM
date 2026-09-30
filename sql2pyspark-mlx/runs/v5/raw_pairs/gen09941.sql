SELECT id, started_at, ROW_NUMBER() OVER (ORDER BY started_at DESC, id) row_seq FROM sessions;
