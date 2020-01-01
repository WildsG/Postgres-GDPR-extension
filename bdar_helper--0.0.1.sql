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

CREATE TABLE IF NOT EXISTS bdar_tables.anon_config(
	command varchar not null,
	value varchar[],
	anon_level varchar not null,
	primary KEY(anon_level, command)
);


insert into bdar_tables.conf(param, value) values('local', 'true');
insert into bdar_tables.conf(param, value) values('delete_wait_time_minutes', '1');

CREATE or replace FUNCTION encrypt_column() 
  RETURNS trigger AS
$$
declare
   v_sql text;
   v_sql1 text;
  _col text := quote_ident(TG_ARGV[0]);
begin
   execute 'select PGP_SYM_ENCRYPT('||'$1'||'.'||TG_ARGV[0]||','||quote_literal(TG_ARGV[1])||')' using new into v_sql;
   NEW:= NEW #= hstore(_col, v_sql);  
   RETURN NEW;
END;
$$
language plpgsql;

create or replace function set_crypted_column(schema_name varchar, table_name varchar, column_name varchar, crypt_key varchar) returns void as
$$
begin
	execute 'create trigger '||schema_name||'_'||table_name||'_'||column_name||'_crypt'||' before insert on '||schema_name||'.'||table_name||' for each row execute procedure encrypt_column('||quote_literal(column_name)||','||quote_literal(crypt_key)||');';
end;
$$
language plpgsql;

create or replace function remove_crypted_column(schema_name varchar, table_name varchar, column_name varchar) returns void as
$$
begin
	execute 'drop trigger '||schema_name||'_'||table_name||'_'||column_name||'_crypt'||' on '||schema_name||'.'||table_name||';';
end;
$$
language plpgsql;


insert into bdar_tables.anon_config(command, value, anon_level) values ('email',array['3', '***', '5'], 'LOW');
insert into bdar_tables.anon_config(command, value, anon_level) values ('phone',array['1','xxxx','3'], 'LOW');
insert into bdar_tables.anon_config(command, value, anon_level) values ('birth',array['month'], 'LOW');
insert into bdar_tables.anon_config(command, value, anon_level) values ('zip', array['50'], 'LOW');


insert into bdar_tables.anon_config(command, value, anon_level) values ('email',array['2', '*****', '4'], 'MED');
insert into bdar_tables.anon_config(command, value, anon_level) values ('phone',array['1','xxxxxx','1'], 'MED');
insert into bdar_tables.anon_config(command, value, anon_level) values ('birth',array['year'], 'MED');
insert into bdar_tables.anon_config(command, value, anon_level) values ('zip', array['100'], 'MED');

insert into bdar_tables.anon_config(command, value, anon_level) values ('email',array['1', '******', '2'], 'HIGH');
insert into bdar_tables.anon_config(command, value, anon_level) values ('phone',array['2','xxxxxx','0'], 'HIGH');
insert into bdar_tables.anon_config(command, value, anon_level) values ('birth',array['decade'], 'HIGH');
insert into bdar_tables.anon_config(command, value, anon_level) values ('zip', array['1000'], 'HIGH');

create or replace function bdar_anonimyze(anon_lvl varchar, view_name varchar, commands varchar[]) returns void as 
$$
declare
	v_sql text;
	counter integer;
	delete_sql text;
	rd record;
	record_count integer;
	column_command text;
	stringified text;
	arr text[];
begin
	counter:= 1;
	record_count:= (select
		count(c.column_name)
	from
		information_schema.tables t
	left join information_schema.columns c on
		t.table_schema = c.table_schema
		and t.table_name = c.table_name
	where
		table_type = 'VIEW'
		and t.table_schema not in ('information_schema','pg_catalog','anon')
		and t.table_name = view_name);
	-- TODO length check
	------------------------------------------------------------------------
	v_sql:= 'create or replace VIEW '||view_name||'_anonimyzed AS
	SELECT ';
    for rd in (select
		c.column_name
	from
		information_schema.tables t
	left join information_schema.columns c on
		t.table_schema = c.table_schema
		and t.table_name = c.table_name
	where
		table_type = 'VIEW'
		and t.table_schema not in ('information_schema','pg_catalog','anon')
		and t.table_name = view_name
	) 
    loop	
    	column_command:= regexp_replace(rd::varchar,'\(|\)', '', 'g');
		IF commands[counter] = 'zip' then
			select array_to_string((select value from bdar_tables.anon_config ac where command = 'zip' and ac.anon_level = anon_lvl), ',', '*') into stringified;
   			column_command:= 'anon.generalize_int4range('||column_command||'::INTEGER,'||stringified||') AS '||column_command;
		END IF;
		IF commands[counter] = 'email' then
			select value from bdar_tables.anon_config ac where command = 'email' and ac.anon_level = anon_lvl into arr;
   			column_command:= 'anon.partial('||column_command||','||arr[1]||','||quote_literal(arr[2])||','||arr[3]||') AS '||column_command;
		END IF;
		IF commands[counter] = 'phone' then
			select value from bdar_tables.anon_config ac where command = 'phone' and ac.anon_level = anon_lvl into arr;
   			column_command:= 'anon.partial('||column_command||','||arr[1]||','||quote_literal(arr[2])||','||arr[3]||') AS '||column_command;
		END IF;
		IF commands[counter] = 'birth' then
			select array_to_string((select value from bdar_tables.anon_config ac where command = 'zip'  and ac.anon_level = anon_lvl), ',', '*') into stringified;
   			column_command:= 'anon.generalize_int4range('||column_command||'::INTEGER,'||stringified||') AS '||column_command;
		END IF;
    	v_sql:= v_sql||column_command||',';
    	counter:= counter + 1;
    end loop;
    v_sql:= LEFT(v_sql,length(v_sql)-1)||' from '||view_name||';';
   	delete_sql:= 'DROP VIEW IF EXISTS '||view_name||'_anonimyzed;';
   	execute delete_sql;
    execute v_sql;
end;
$$
LANGUAGE PLPGSQL;

create or replace function bdar_anonimyze_column(anon_lvl varchar, view_name varchar, column_name varchar, v_command varchar) returns void as 
$$
declare
	v_sql text;
	delete_sql text;
	rd record;
	column_command text;
	stringified text;
	arr text[];
begin
	------------------------------------------------------------------------
	--SELECT anon.load();
	v_sql:= 'create or replace VIEW '||view_name||'_anonimyzed AS
	SELECT ';
    for rd in (select
		c.column_name
	from
		information_schema.tables t
	left join information_schema.columns c on
		t.table_schema = c.table_schema
		and t.table_name = c.table_name
	where
		table_type = 'VIEW'
		and t.table_schema not in ('information_schema','pg_catalog','anon')
		and t.table_name = view_name
	) 
    loop	
    	column_command:= regexp_replace(rd::varchar,'\(|\)', '', 'g');
    	if column_command = column_name then
		IF v_command = 'zip' then
			select array_to_string((select value from bdar_tables.anon_config ac where command = 'zip' and ac.anon_level = anon_lvl), ',', '*') into stringified;
   			column_command:= 'anon.generalize_int4range('||column_command||'::INTEGER,'||stringified||') AS '||column_command;
		END IF;
		IF v_command = 'email' then
			select value from bdar_tables.anon_config ac where command = 'email' and ac.anon_level = anon_lvl into arr;
   			column_command:= 'anon.partial('||column_command||','||arr[1]||','||quote_literal(arr[2])||','||arr[3]||') AS '||column_command;
		END IF;
		IF v_command = 'phone' then
			select value from bdar_tables.anon_config ac where command = 'phone' and ac.anon_level = anon_lvl into arr;
   			column_command:= 'anon.partial('||column_command||','||arr[1]||','||quote_literal(arr[2])||','||arr[3]||') AS '||column_command;
		END IF;
		IF v_command = 'birth' then
			select array_to_string((select value from bdar_tables.anon_config ac where command = 'zip'  and ac.anon_level = anon_lvl), ',', '*') into stringified;
   			column_command:= 'anon.generalize_int4range('||column_command||'::INTEGER,'||stringified||') AS '||column_command;
		END IF;
		end if;
    	v_sql:= v_sql||column_command||',';
    end loop;
    v_sql:= LEFT(v_sql,length(v_sql)-1)||' from '||view_name||';';
   	delete_sql:= 'DROP VIEW IF EXISTS '||view_name||'_anonimyzed;';
   	execute delete_sql;
    execute v_sql;
end;
$$
LANGUAGE PLPGSQL;

create or replace function add_anon_rule(v_command varchar,value varchar[],  anon_lvl varchar) 
returns text as $$
declare
    v_id text;
begin
	insert into bdar_tables.anon_config(command, value, anon_level) values (v_command,value, anon_lvl)
	returning v_command into v_id;
return v_id;
end;
$$
LANGUAGE PLPGSQL;

create or replace function remove_anon_rule(v_command varchar,  anon_lvl varchar) 
returns text as $$
declare
    v_id text;
begin
	delete from bdar_tables.anon_config where v_command = command and anon_level = anon_lvl
	returning v_command into v_id;
return v_id;
end;
$$
LANGUAGE PLPGSQL;


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
