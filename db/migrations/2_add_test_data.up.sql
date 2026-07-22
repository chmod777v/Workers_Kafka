INSERT INTO tasks (id, token, message)
VALUES (1, 'fk8a;', 'Hi world')
ON CONFLICT DO NOTHING;
