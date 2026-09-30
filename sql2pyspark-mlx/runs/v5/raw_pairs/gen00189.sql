SELECT id, status FROM payments WHERE status = 'SUCCESS' AND status = 'FAILED' AND amount BETWEEN 430 AND 560 ORDER BY status DESC, id;
