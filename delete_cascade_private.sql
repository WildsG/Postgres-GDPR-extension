DROP SCHEMA bdar cascade;

CREATE SCHEMA bdar;

CREATE TABLE account(
   id serial PRIMARY KEY,
   username VARCHAR (50) UNIQUE NOT NULL,
   password_hash VARCHAR NOT NULL,
   firstname varchar(50) not null,
   lastname varchar(100) not null,
   email VARCHAR (355) UNIQUE NOT NULL,
   phone_number varchar(15) unique not null,
   created_on TIMESTAMP NOT NULL,
   last_login TIMESTAMP
);

CREATE TABLE credit_card(
   id serial PRIMARY KEY,
   cc varchar UNIQUE NOT NULL,
   cc_num varchar NOT NULL,
   holder_name VARCHAR NOT NULL,
   expire_date TIMESTAMP NOT null,
   account_id integer references account(id)
);

create table product(
   id serial PRIMARY KEY,
   product_name varchar(100) NOT NULL,
   product_type varchar(20),
   description varchar(255),
   stock integer NOT null,
   price numeric not null
);

create table review(
	id serial primary key,
	account_id integer references account(id),
	product_id integer references product(id),
	message varchar(255),
	rating integer not null
);

create table purchase_history(
	id serial primary key,
	account_id integer references account(id),
	product_id integer references product(id),
	purchase_date TIMESTAMP NOT null
);

create table address(
	id serial primary key,
	name varchar(100) not null,
	address1 varchar(100) not null,
	address2 varchar(100),
	city varchar(100) not null,
	state_province varchar(100) not null,
	postal_code varchar(10) not null,
	account_id integer references account(id)
);



INSERT INTO account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('Arturas', MD5('password'),'Arturas', 'Dulka' ,'arturas.test@gmail.com','+37000000000', CURRENT_TIMESTAMP,current_timestamp + (20 * interval '1 minute'));

INSERT INTO account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('TomasD', MD5('password'),'Tomas', 'Dulka' ,'tomas.test@gmail.com','+37000000001', CURRENT_TIMESTAMP,current_timestamp + (20 * interval '1 minute'));

INSERT INTO credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
(987654321,111,'Arturas Dulka', current_timestamp + (5 * interval '1 year'),1);

insert into product(product_name, product_type, description, stock, price) values 
('Samsung Galaxy 7', 'smartphone', 'good, reliable, but weak battery', 1, 80);
insert into product(product_name, product_type, description, stock, price) values 
('Samsung Galaxy 8', 'smartphone', 'better than 7 , good, reliable, but weak battery', 1, 180);
insert into product(product_name, product_type, description, stock, price) values 
('Samsung Galaxy 9', 'smartphone', 'better than 8 , good, reliable, but weak battery', 1, 380);

insert into review(account_id, product_id,message, rating) values 
(1, 2, 'i liked it', 9);
insert into review(account_id, product_id,message, rating) values 
(1, 1, 'not so good. quite old already', 7);

insert into purchase_history(account_id, product_id, purchase_date) values
(1,2, current_timestamp);
insert into purchase_history(account_id, product_id, purchase_date) values
(1,1, current_timestamp);

insert into bdar.address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('Home', 'Konstitucijos 00-0', null, 'Vilnius', 'Vilniaus apskritis', '45678', 1);
insert into bdar.address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('Home', 'Konstitucijos 00-0', null, 'Vilnius', 'Vilniaus apskritis', '44009', 2);
insert into bdar.address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('Home', 'Konstitucijos 00-0', null, 'Vilnius', 'Vilniaus apskritis', '46888', 6);


create or replace function delete_cascade_private(p_schema varchar, p_table varchar, p_key varchar, p_recursion varchar[] default null, foreign_column varchar default null)
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
                recnum:= recnum +delete_cascade_private(rx.foreign_table_schema::varchar, rx.foreign_table_name::varchar, rd.key::varchar, p_recursion||v_recursion_key,  rx.foreign_column_name::varchar);
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
   		 v_insert_sql:= FORMAT('insert into bdar_tables.delayed_delete_rows (schema_name, table_name, record_id, delete_on) VALUES(%s,%s,%s,%s)',
   							  quote_literal(p_schema),quote_literal(p_table),p_key, quote_literal(current_timestamp + (10 * interval '1 minute')));   		
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

select delete_cascade_private('bdar', 'account', '1');