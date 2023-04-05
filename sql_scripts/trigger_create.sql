CREATE TABLE log_table (
  operation CHAR(6) NOT NULL, -- insert, update, or delete
  table_name VARCHAR(50) NOT NULL,
  row_data JSONB NOT NULL,
  timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION log_changes() RETURNS TRIGGER AS $$
DECLARE
  pk_value INTEGER;
BEGIN
  -- Get the primary key value for the row
  pk_value = NEW.id; -- Replace "id" with the name of your primary key column
  
  IF (TG_OP = 'INSERT') THEN
    INSERT INTO log_table (operation, table_name, row_data)
    VALUES ('insert', TG_TABLE_NAME, jsonb_build_object(
      'id', pk_value,
      'data', to_jsonb(NEW)
    ));
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO log_table (operation, table_name, row_data)
    VALUES ('update', TG_TABLE_NAME, jsonb_build_object(
      'id', pk_value,
      'old_data', to_jsonb(OLD),
      'new_data', to_jsonb(NEW)
    ));
  ELSIF (TG_OP = 'DELETE') THEN
    INSERT INTO log_table (operation, table_name, row_data)
    VALUES ('delete', TG_TABLE_NAME, jsonb_build_object(
      'id', pk_value,
      'data', to_jsonb(OLD)
    ));
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_table_changes
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW
EXECUTE FUNCTION log_changes();


SELECT * FROM information_schema.triggers WHERE event_object_table = 'products';
DROP TRIGGER log_table_changes ON products;
DROP FUNCTION log_changes();
