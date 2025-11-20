---First test

CREATE TABLE test_users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

INSERT INTO test_users VALUES (1, 'John Doe', 'john@example.com');
INSERT INTO test_users VALUES (2, 'Jane Smith', 'jane@example.com');

INSERT INTO test_users VALUES (3, 'John Roe', 'johnroe@example.com');

---test 