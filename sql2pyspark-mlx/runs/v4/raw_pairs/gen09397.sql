SELECT id, device FROM sessions WHERE browser IN ('Edge', 'Safari', 'Firefox') INTERSECT SELECT id, device FROM sessions WHERE browser = 'Firefox';
