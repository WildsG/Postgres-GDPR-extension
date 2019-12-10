CREATE SCHEMA bdar_tables;
 
CREATE TABLE IF NOT EXISTS bdar_tables.private_entities(
	id serial primary key,
	schema_name varchar not null,
	table_name varchar not null
);

CREATE TABLE IF NOT EXISTS bdar_tables.delayed_delete_rows(
	id serial primary key,
	schema_name varchar not null,
	table_name varchar not null,
	record_id integer not null,
	delete_on timestamp not null
);

select * from bdar_tables.delayed_delete_rows;
delete from bdar_tables.delayed_delete_rows;

CREATE EXTENSION pg_cron;


-- this needed to work locally
UPDATE cron.job SET nodename = '';


SELECT cron.schedule('*/2 * * * *', 

$$
	FOR rec IN
        SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id ASC
    LOOP
        raise notice '%',rec.id;
    END LOOP;
	DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now();
    v_sql := 'delete from '||p_schema||'.'||p_table||' where '||v_primary_key||'='||quote_literal(p_key);

$
);


SELECT cron.unschedule(6);
select * from cron.job

DO $$
declare
    rec record;
    v_select_q varchar;
    ret_val varchar;
BEGIN
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    raise notice '%',v_select_q;
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    raise notice '%',ret_val;
END LOOP;
END; $$

