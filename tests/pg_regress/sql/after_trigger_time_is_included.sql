-- setup
CREATE TABLE trg_test(id int);

CREATE FUNCTION trg_sleep() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_sleep(2);
  RETURN NULL;
END $$;

CREATE TRIGGER trg_after AFTER INSERT ON trg_test
  FOR EACH ROW EXECUTE FUNCTION trg_sleep();

-- statement under test, run once
INSERT INTO trg_test VALUES (1);

-- assertion
SELECT calls, total_rows, total_time_ms >= 1500 AS includes_trigger_time
FROM get_query_stats()
WHERE query_string LIKE 'INSERT INTO trg_test%';

-- cleanup
DROP TABLE trg_test;
DROP FUNCTION trg_sleep();