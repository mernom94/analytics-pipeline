-- Pre-create partitions for the next 14 days
-- This runs once on docker-entrypoint-initdb.d
-- The Python script handles ongoing partition management

DO $$
DECLARE
  d date;
  partition_name text;
  start_val text;
  end_val text;
BEGIN
  FOR i IN -1..14 LOOP
    d := CURRENT_DATE + i;
    partition_name := 'raw_events_' || to_char(d, 'YYYY_MM_DD');
    start_val := d::text;
    end_val := (d + 1)::text;

    -- Only create if the parent table exists and partition doesn't
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'raw_events') THEN
      IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name) THEN
        EXECUTE format(
          'CREATE TABLE IF NOT EXISTS %I PARTITION OF raw_events FOR VALUES FROM (%L) TO (%L)',
          partition_name, start_val, end_val
        );
        RAISE NOTICE 'Created partition: %', partition_name;
      END IF;
    END IF;
  END LOOP;
END $$;
