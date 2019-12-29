-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bdar_helper" to load this file. \quit

CREATE SCHEMA if not exists bdar_tables;
 
CREATE TABLE IF NOT EXISTS bdar_tables.private_entities(
	id serial primary key,
	schema_name varchar not null,
	table_name varchar not null,
	unique(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS bdar_tables.delayed_delete_rows(
	id serial primary key,
	schema_name varchar not null,
	table_name varchar not null,
	record_id integer not null,
	delete_on timestamp not null
);

CREATE TABLE IF NOT EXISTS bdar_tables.conf(
	id serial primary key,
	param varchar not null,
	value varchar not null
);

CREATE TABLE IF NOT EXISTS bdar_tables.cron_log(
	id serial primary key,
	query varchar not null,
	executed timestamp not null
);
CREATE TABLE IF NOT EXISTS bdar_tables.postgres_log
(
  log_time timestamp(3) with time zone,
  user_name text,
  database_name text,
  process_id integer,
  connection_from text,
  session_id text,
  session_line_num bigint,
  command_tag text,
  session_start_time timestamp with time zone,
  virtual_transaction_id text,
  transaction_id bigint,
  error_severity text,
  sql_state_code text,
  message text,
  detail text,
  hint text,
  internal_query text,
  internal_query_pos integer,
  context text,
  query text,
  query_pos integer,
  location text,
  application_name text,
  PRIMARY KEY (session_id, session_line_num)
);

insert into bdar_tables.conf(param, value) values('local', 'true');
insert into bdar_tables.conf(param, value) values('delete_wait_time_minutes', '1');

create or replace function bdar_show_activity_log() RETURNS TABLE (
  log_time timestamp(3) with time zone,
  user_name text,
  database_name text,
  process_id integer,
  connection_from text,
  session_id text,
  session_line_num bigint,
  command_tag text,
  session_start_time timestamp with time zone,
  virtual_transaction_id text,
  transaction_id bigint,
  error_severity text,
  sql_state_code text,
  message text,
  detail text,
  hint text,
  internal_query text,
  internal_query_pos integer,
  context text,
  query text,
  query_pos integer,
  location text,
  application_name text
)
AS $$
declare
    v_select_q varchar;
   	dir varchar;
begin 
	CREATE TEMP TABLE tmp_table 
	ON COMMIT drop AS
	SELECT * FROM bdar_tables.postgres_log WITH NO DATA;
	select  setting from pg_catalog.pg_settings where name = 'data_directory' into dir;
	v_select_q := 'copy tmp_table from program '||' ''cat '||dir||'/log/*.csv'' with csv';
	raise notice '%', v_select_q;
	execute v_select_q ;
	INSERT INTO bdar_tables.postgres_log 
	SELECT * FROM tmp_table t where not exists (select * from bdar_tables.postgres_log p where p.session_id = t.session_id and p.session_line_num = t.session_line_num);
	RETURN query select * from bdar_tables.postgres_log pl;
end;
$$
LANGUAGE PLPGSQL;

create or replace function add_private_entity(p_schema varchar, p_table varchar) 
returns integer as $$
declare
    v_id integer;
begin
	INSERT INTO bdar_tables.private_entities (schema_name, table_name) values (p_schema, p_table)
	returning id into v_id;
return v_id;
end;
$$
LANGUAGE PLPGSQL;

create or replace function remove_private_entity(p_schema varchar, p_table varchar) 
returns integer as $$
declare
    v_id integer;
begin
	delete from bdar_tables.private_entities where schema_name = p_schema and table_name = p_table 
	returning id into v_id;
return v_id;
end;
$$
LANGUAGE PLPGSQL;

create or replace function update_parameter(parameter_name varchar, parameter_value varchar) 
returns varchar as $$
declare
    v_id varchar;
begin
	update bdar_tables.conf set value = parameter_value where param = parameter_name
	returning parameter_value into v_id;
	if(v_id is null) then
		raise exception 'no such parameter with % name', parameter_name;
	end if;
return v_id;
end;
$$
LANGUAGE PLPGSQL;

-- This fucntions is for cron use
CREATE or replace FUNCTION bdar_delete() RETURNS integer AS $$
declare
    rec record;
    v_select_q varchar;
BEGIN
INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat', current_timestamp);
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows  WHERE delete_on < now() ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (v_select_q, current_timestamp);
END LOOP;
RETURN 0;
END; $$
LANGUAGE PLPGSQL;


-- DEPENDENCIES
SELECT cron.schedule('* * * * *', $job$
do $$ declare
    rec record;
    v_select_q varchar;
BEGIN
INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat1', current_timestamp);
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows  WHERE delete_on < now() ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (v_select_q, current_timestamp);
END LOOP;
END; $$
$job$);


-- delete or disconnect all records related to one selected and also selected
-- basic configuration: if column not null - deletes else if column nullable, disconects entities
create or replace function bdar_forget(p_schema varchar, p_table varchar, p_key varchar, p_recursion varchar[] default null, foreign_column varchar default null)
returns integer as $$
declare
    rx record;
    rd record;
    v_sql varchar;
    v_is_nullable varchar;
    v_recursion_key varchar;
    recnum integer;
    v_primary_key varchar;
    v_rows integer;
   	ret_val varchar;
    v_insert_sql varchar;
    v_delete_after integer;
begin
	raise notice 'Info %',foreign_column;
    recnum := 0;
    select ccu.column_name into v_primary_key
        from
        information_schema.table_constraints  tc
        join information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name and ccu.constraint_schema=tc.constraint_schema
        and tc.constraint_type='PRIMARY KEY'
        and tc.table_name=p_table
        and tc.table_schema=p_schema;

    for rx in (
        select kcu.table_name as foreign_table_name, 
        kcu.column_name as foreign_column_name, 
        kcu.table_schema foreign_table_schema,
        kcu2.column_name as foreign_table_primary_key
        from information_schema.constraint_column_usage ccu
        join information_schema.table_constraints tc on tc.constraint_name=ccu.constraint_name and tc.constraint_catalog=ccu.constraint_catalog and ccu.constraint_schema=ccu.constraint_schema 
        join information_schema.key_column_usage kcu on kcu.constraint_name=ccu.constraint_name and kcu.constraint_catalog=ccu.constraint_catalog and kcu.constraint_schema=ccu.constraint_schema
        join information_schema.table_constraints tc2 on tc2.table_name=kcu.table_name and tc2.table_schema=kcu.table_schema
        join information_schema.key_column_usage kcu2 on kcu2.constraint_name=tc2.constraint_name and kcu2.constraint_catalog=tc2.constraint_catalog and kcu2.constraint_schema=tc2.constraint_schema
        where ccu.table_name=p_table  and ccu.table_schema=p_schema
        and TC.CONSTRAINT_TYPE='FOREIGN KEY'
        and tc2.constraint_type='PRIMARY KEY'
	)
    loop
        v_sql := 'select '||rx.foreign_table_primary_key||' as key from '||rx.foreign_table_schema||'.'||rx.foreign_table_name||'
            where '||rx.foreign_column_name||'='||quote_literal(p_key)||' for update';
        raise notice '%',v_sql;
        --found a foreign key, now find the primary keys for any data that exists in any of those tables.
        for rd in execute v_sql
        loop
            v_recursion_key=rx.foreign_table_schema||'.'||rx.foreign_table_name||'.'||rx.foreign_column_name||'='||rd.key;
             raise notice '%',v_recursion_key;
            if (v_recursion_key = any (p_recursion)) then
                raise notice 'Avoiding infinite loop';
            else
                raise notice 'Recursing to %,%',rx.foreign_table_name, rd.key;
                recnum:= recnum +bdar_forget(rx.foreign_table_schema::varchar, rx.foreign_table_name::varchar, rd.key::varchar, p_recursion||v_recursion_key,  rx.foreign_column_name::varchar);
            end if;
        end loop;
    end loop;
    begin
    --actually delete original record.\
    v_is_nullable := 'select is_nullable from INFORMATION_SCHEMA.columns where table_schema = '||quote_literal(p_schema)||' and table_name ='||quote_literal(p_table)||' and column_name ='||quote_literal(foreign_column);
   	if(v_is_nullable is not null) then
   		execute v_is_nullable into ret_val;
   	else
   		raise notice 'test:: %',v_is_nullable; 
   	end if;
   	raise notice 'Info %.% %',p_schema,p_table,foreign_column;
   
    v_sql := 'delete from '||p_schema||'.'||p_table||' where '||v_primary_key||'='||quote_literal(p_key);
   	if(ret_val = 'YES') then
   		v_sql := 'update '||p_schema||'.'||p_table||' set '||foreign_column||'= NULL where '||v_primary_key||'='||quote_literal(p_key);
   		v_delete_after:= (select c.value from bdar_tables.conf c where c.param = 'delete_wait_time_minutes');
		v_insert_sql:= FORMAT('insert into bdar_tables.delayed_delete_rows (schema_name, table_name, record_id, delete_on) VALUES(%s,%s,%s,%s)',
   							  quote_literal(p_schema),quote_literal(p_table),p_key, quote_literal(current_timestamp + (v_delete_after * interval '1 minute')));
     	raise notice '%',v_insert_sql;
   		execute v_insert_sql;
   end if;
    
   	raise notice '%',v_sql;
    execute v_sql;
   	--raise notice 'Deleting %.% %=%',p_schema,p_table,v_primary_key,p_key;
    get diagnostics v_rows= row_count;
    recnum:= recnum +v_rows;
   
    exception when others then recnum=0;
    end;

    return recnum;
end;
$$
language PLPGSQL;

-- delete or disconnect all records related to one selected and also selected
-- Configurable: add entities to private_entities table, so it disconnects that data, other entities will not be private and will just immediately delete
create or replace function bdar_forget_configured(p_schema varchar, p_table varchar, p_key varchar, p_recursion varchar[] default null, foreign_column varchar default null)
 returns integer as $$
declare
    rx record;
    rd record;
    v_sql varchar;
    v_is_nullable varchar;
    v_recursion_key varchar;
    recnum integer;
    v_primary_key varchar;
    v_rows integer;
   	ret_val varchar;
    v_insert_sql varchar;
    v_delete_after integer;
begin
	raise notice 'Info %',foreign_column;
    recnum := 0;
    select ccu.column_name into v_primary_key
        from
        information_schema.table_constraints  tc
        join information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name and ccu.constraint_schema=tc.constraint_schema
        and tc.constraint_type='PRIMARY KEY'
        and tc.table_name=p_table
        and tc.table_schema=p_schema;

    for rx in (
        select kcu.table_name as foreign_table_name, 
        kcu.column_name as foreign_column_name, 
        kcu.table_schema foreign_table_schema,
        kcu2.column_name as foreign_table_primary_key
        from information_schema.constraint_column_usage ccu
        join information_schema.table_constraints tc on tc.constraint_name=ccu.constraint_name and tc.constraint_catalog=ccu.constraint_catalog and ccu.constraint_schema=ccu.constraint_schema 
        join information_schema.key_column_usage kcu on kcu.constraint_name=ccu.constraint_name and kcu.constraint_catalog=ccu.constraint_catalog and kcu.constraint_schema=ccu.constraint_schema
        join information_schema.table_constraints tc2 on tc2.table_name=kcu.table_name and tc2.table_schema=kcu.table_schema
        join information_schema.key_column_usage kcu2 on kcu2.constraint_name=tc2.constraint_name and kcu2.constraint_catalog=tc2.constraint_catalog and kcu2.constraint_schema=tc2.constraint_schema
        where ccu.table_name=p_table  and ccu.table_schema=p_schema
        and TC.CONSTRAINT_TYPE='FOREIGN KEY'
        and tc2.constraint_type='PRIMARY KEY'
	)
    loop
        v_sql := 'select '||rx.foreign_table_primary_key||' as key from '||rx.foreign_table_schema||'.'||rx.foreign_table_name||'
            where '||rx.foreign_column_name||'='||quote_literal(p_key)||' for update';
        raise notice '%',v_sql;
        --found a foreign key, now find the primary keys for any data that exists in any of those tables.
        for rd in execute v_sql
        loop
            v_recursion_key=rx.foreign_table_schema||'.'||rx.foreign_table_name||'.'||rx.foreign_column_name||'='||rd.key;
             raise notice '%',v_recursion_key;
            if (v_recursion_key = any (p_recursion)) then
                raise notice 'Avoiding infinite loop';
            else
                raise notice 'Recursing to %,%',rx.foreign_table_name, rd.key;
                recnum:= recnum +bdar_forget_configured(rx.foreign_table_schema::varchar, rx.foreign_table_name::varchar, rd.key::varchar, p_recursion||v_recursion_key,  rx.foreign_column_name::varchar);
            end if;
        end loop;
    end loop;
    begin
    --actually delete original record.\
   	raise notice 'Info %.% %',p_schema,p_table,foreign_column;
   
    v_sql := 'delete from '||p_schema||'.'||p_table||' where '||v_primary_key||'='||quote_literal(p_key);
    select table_name from bdar_tables.private_entities 
    where bdar_tables.private_entities.table_name in(p_table) 
    and bdar_tables.private_entities.schema_name in(p_schema) into ret_val;
    raise notice 'name: %',ret_val;
    v_sql := 'delete from '||p_schema||'.'||p_table||' where '||v_primary_key||'='||quote_literal(p_key);
    if(ret_val is not null) then
   		 v_sql := 'update '||p_schema||'.'||p_table||' set '||foreign_column||'= NULL where '||v_primary_key||'='||quote_literal(p_key);
		 v_delete_after:= (select c.value from bdar_tables.conf c where c.param = 'delete_wait_time_minutes');
   		 v_insert_sql:= FORMAT('insert into bdar_tables.delayed_delete_rows (schema_name, table_name, record_id, delete_on) VALUES(%s,%s,%s,%s)',
   							  quote_literal(p_schema),quote_literal(p_table),p_key, quote_literal(current_timestamp + (v_delete_after * interval '1 minute')));   		
   		execute v_insert_sql;
    end if;
   	raise notice '%',v_sql;
    execute v_sql;
   	--raise notice 'Deleting %.% %=%',p_schema,p_table,v_primary_key,p_key;
    get diagnostics v_rows= row_count;
    recnum:= recnum +v_rows;
   
    exception when others then recnum=0;
    end;

    return recnum;
end;
$$
language PLPGSQL;

-- needed for localhost. Otherwise does not work

UPDATE cron.job SET nodename = '';
