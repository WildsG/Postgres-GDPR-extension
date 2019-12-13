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
	param varchar primary key,
	value varchar not null
);

drop table bdar_tables.conf
insert into bdar_tables.conf(param, value) values('delete_wait_time_minutes', '1');

insert into bdar_tables.conf(param, value) values('local', 'true');
update bdar_tables.conf set value = '3' where param = 'delete_wait_time_minutes';
select * from bdar_tables.conf
insert into bdar_tables.private_entities (schema_name, table_name) values ('p_schema', p_table)

WITH deleted AS (DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now() RETURNING *),
deleted_main as (delete from )
insert into bdar_tables.cron_log (query, executed) (select id, current_timestamp FROM deleted)

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


SELECT cron.schedule('*/2 * * * *', 

$$
	FOR rec IN
        SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id ASC
    LOOP
        raise notice '%',rec.id;
    END LOOP;
	DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now();
    v_sql := 'delete from '||p_schema||'.'||p_table||' where '||v_primary_key||'='||quote_literal(p_key);
$$
);	
SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc
	select * FROM bdar_tables.delayed_delete_rows;
do $$
declare
    rec record;
    v_select_q varchar;
BEGIN
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (v_select_q, current_timestamp);
END LOOP;
END; $$



CREATE or replace FUNCTION bdar_delete() RETURNS integer AS $$
declare
    rec record;
    v_select_q varchar;
BEGIN
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (v_select_q, current_timestamp);
END LOOP;
RETURN 0;
END; $$
SELECT cron.schedule('* * * * *', $$SELECT bdar_delete();$$);

SELECT cron.schedule('* * * * *', $$
declare
    rec record;
    v_select_q varchar;
BEGIN
	INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat', current_timestamp);
END; $$);

CREATE or replace FUNCTION simple() RETURNS integer AS $$
declare
    rec record;
    v_select_q varchar;
BEGIN
	INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat', current_timestamp);
RETURN 0;
END; $$
LANGUAGE PLPGSQL;

Select simple();


SELECT bdar_delete()
INSERT INTO bdar_tables.cron_log(query, executed) ('heartbeat', current_timestamp)
LANGUAGE PLPGSQL;
select * from cron.job ;
select * from bdar_tables.cron_log;
select * from bdar_tables.conf;
select * from bdar_tables.delayed_delete_rows;
delete from bdar_tables.delayed_delete_rows;
delete from bdar_tables.cron_log cl;
INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat', current_timestamp);

	SELECT bdar_delete();
SELECT * FROM bdar_tables.delayed_delete_rows  WHERE delete_on < now() ORDER BY id asc

SELECT cron.schedule('* * * * *', $$
INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat', current_timestamp);
INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('heartbeat after', current_timestamp);

$$);

CREATE EXTENSION pg_cron;
select * from cron.job;
SELECT cron.unschedule(1);
SELECT cron.unschedule(2);
SELECT cron.unschedule(3);
SELECT cron.unschedule(4);
 SELECT cron.schedule('* * * * *', 
$$INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('delete', current_timestamp);$$);
UPDATE cron.job SET nodename = '';

 



CREATE or replace FUNCTION bdar_delete() RETURNS integer AS $$
declare
    rec record;
    v_select_q varchar;
BEGIN
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (v_select_q, current_timestamp);
END LOOP;
RETURN 0;
END; $$
LANGUAGE PLPGSQL;
 
 SELECT cron.schedule('* * * * *', 
$$INSERT INTO bdar_tables.cron_log(query, executed) VALUES ( (WITH deleted AS (DELETE FROM table WHERE condition IS TRUE RETURNING *) SELECT count(*) FROM deleted)
, current_timestamp);$$);


INSERT INTO bdar_tables.cron_log(query, executed) VALUES (
(WITH deleted AS (DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now() RETURNING *) SELECT count(*) FROM deleted)
, current_timestamp);
-- this needed to work locally





WITH deleted AS (DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now() RETURNING *) SELECT count(*) FROM deleted

SELECT cron.schedule('* * * * *', 
$$
declare
    rec record;
    v_select_q varchar;
BEGIN
  INSERT INTO bdar_tables.cron_log(query, executed) VALUES ('delet1', current_timestamp);
  DELETE FROM bdar_tables.delayed_delete_rows WHERE delete_on < now();
END; $$
);


declare
    rec record;
    v_select_q varchar;
BEGIN
FOR rec IN
    (SELECT * FROM bdar_tables.delayed_delete_rows ORDER BY id asc)
loop
	v_select_q := 'delete from '||rec.schema_name||'.'||rec.table_name||' where id='||quote_literal(rec.record_id);
    execute v_select_q;
    delete from bdar_tables.delayed_delete_rows ddr where rec.id = ddr.id;
    INSERT INTO bdar_tables.cron_log(query, executed) VALUES (quote_literal(v_select_q), current_timestamp);
END LOOP;
END; 


SELECT cron.unschedule(6);
select * from cron.job

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

do $$ declare
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
END; $$


do $$ 
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
END; $$



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

select add_private_entity('bdar', 'address');

insert into bdar_tables.private_entities(schema_name, table_name) values
('bdar', 'address');
insert into bdar_tables.private_entities(schema_name, table_name) values
('bdar', 'credit_card') returning id;

select remove_private_entity('bdar', 'address');


select * from bdar_tables.cron_log

delete from bdar.account a where id = 1;

select * from bdar_tables.private_entities pe;
select * from bdar_tables.delayed_delete_rows;

select * from bdar_tables.cron_log;


do $$ declare
    v integer;
BEGIN
	v_delete_after:= (select c.value from bdar_tables.conf c where c.param = 'delete_wait_time_minutes');
	raise notice '%', v;
END; $$
