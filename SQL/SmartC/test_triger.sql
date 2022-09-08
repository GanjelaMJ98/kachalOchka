CREATE SCHEMA test;

CREATE TABLE test.fulltable(
    tabnum INT,
    name TEXT,
    age INT
);

CREATE TABLE test.clear(
    tabnum INT,
    name TEXT
);
DROP FUNCTION add_clear_data()  CASCADE
CREATE OR REPLACE FUNCTION add_clear_data()  RETURNS trigger  AS
$$
BEGIN
        INSERT INTO test.clear SELECT tabnum, name FROM test.fulltable;
        RETURN NULL;
END
$$
LANGUAGE 'plpgsql';



CREATE TRIGGER view_insert
    AFTER INSERT ON test.fulltable
    FOR STATEMENT
    EXECUTE PROCEDURE add_clear_data();


INSERT INTO test.fulltable VALUES (2,'LOL',23),(3,'KEK',23),(1,'CHEBURECK',23)