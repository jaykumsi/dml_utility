create table edata.control_Tbl(control_id integer
,load_name varchar(100)
,Oracle_table_name varchar(100)
,Target_Table_name varchar(100)
,pk_csv_cols varchar(500)
,key_audit_table varchar(100)
,last_run_date_time timestamp
,status_flg varchar(10));

create table edata.control_log_Tbl(control_log_id integer
,control_id integer
,run_Date timestamp
,insert_count integer
,update_count integer
,delete_count integer
,status_flg varchar(10));

CREATE OR REPLACE PROCEDURE insert_into_control_table(
  p_control_id INTEGER,
  p_load_name VARCHAR(100),
  p_oracle_table_name VARCHAR(100),
  p_target_table_name VARCHAR(100),
  p_pk_csv_cols VARCHAR(500),
  p_key_audit_table VARCHAR(100),
  p_last_run_date_time TIMESTAMP,
  p_status_flg VARCHAR(10)
)
AS $$
BEGIN
  INSERT INTO control_Tbl (
    control_id,
    load_name,
    Oracle_table_name,
    Target_Table_name,
    pk_csv_cols,
    key_audit_table,
    last_run_date_time,
    status_flg
  )
  VALUES (
    p_control_id,
    p_load_name,
    p_oracle_table_name,
    p_target_table_name,
    p_pk_csv_cols,
    p_key_audit_table,
    p_last_run_date_time,
    p_status_flg
  );
  
  COMMIT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE edata.DML_Utility(p_table_name VARCHAR(100))
AS
  v_insert_count INTEGER;
  v_update_count INTEGER;
  v_delete_count INTEGER;
BEGIN
  INSERT INTO edata.key_audit_table (pk_col1, pk_col2, dml_type, dms_date)
  SELECT pk_col1, pk_col2, dml_type, dms_date
  FROM naviste_dms
  WHERE dms_date >= (SELECT last_run_date_time FROM control_Tbl WHERE target_table_name = p_table_name);

  UPDATE edata.control_Tbl
  SET last_run_date_time = systimestamp
  WHERE target_table_name = p_table_name;

  SELECT COUNT(*) INTO v_insert_count
  FROM edata.key_audit_table
  WHERE dml_type = 'INSERT' AND dms_date = systimestamp;

  SELECT COUNT(*) INTO v_update_count
  FROM edata.key_audit_table
  WHERE dml_type = 'UPDATE' AND dms_date = systimestamp;

  SELECT COUNT(*) INTO v_delete_count
  FROM edata.key_audit_table
  WHERE dml_type = 'DELETE' AND dms_date = systimestamp;

  INSERT INTO control_log (control_log_id, control_id, last_run_date_time, v_insert_count, v_update_count, v_delete_count, flag)
  SELECT control_log_id.seq, control_id, last_run_date_time, v_insert_count, v_update_count, v_delete_count, 'Y'
  FROM edata.control_Tbl
  WHERE last_run_date_time = systimestamp
    AND target_table_name = p_table_name;

EXCEPTION
  WHEN OTHERS THEN
    RAISE SQLCODE || SQLERRM;
END;
