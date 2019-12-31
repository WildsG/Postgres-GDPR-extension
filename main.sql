-- "relational database schema eshop" google this for relational db examples."teest
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
('Arturas1', MD5('password'),'Arturas', 'Dulka' ,'arturas1.test@gmail.com','+37000000002', CURRENT_TIMESTAMP,current_timestamp + (20 * interval '1 minute'));


INSERT INTO account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('TomasD', MD5('password'),'Tomas', 'Dulka' ,'tomas.test@gmail.com','+37000000001', CURRENT_TIMESTAMP,current_timestamp + (20 * interval '1 minute'));

INSERT INTO credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
('1000 1234 5678 9010','111','Arturas Dulka', current_timestamp + (5 * interval '1 year'),1);

INSERT INTO credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
('1000 1234 5678 9011','112','Arturas Dulka', current_timestamp + (5 * interval '1 year'),1);

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
('Home', 'Konstitucijos 00-0', null, 'Vilnius', 'Vilniaus apskritis', '44009', 3);
insert into bdar.address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('Home', 'Konstitucijos 00-0', null, 'Vilnius', 'Vilniaus apskritis', '46888', 4);

create view persons_view as 
select aa.id, aa.firstname, aa.lastname, aa.email,aa.phone_number, ad."name",ad.address1, ad.address2, ad.city, ad.state_province, ad.postal_code from account aa
left join address ad on ad.account_id = aa.id;

--SELECT is_nullable FROM INFORMATION_SCHEMA.columns where table_schema = 'bdar' and table_name = 'credit_card' and column_name = 'account_id';


select * from account;
select * from credit_card;
select * from product;
select * from review;
select * from purchase_history;
select * from address;

--create extension pgcrypto;

--SELECT * FROM pg_available_extensions
drop extension bdar_helper;
drop schema bdar_tables;
create extension pg_cron;
create extension bdar_helper;
select bdar_forget('bdar', 'account', '1');
select bdar_forget_configured('bdar', 'account', '1');
drop extension pg_cron;

select * from bdar_show_activity_log();




select * from bdar_tables.conf c;

select update_parameter('delete_wait_time_minutes', '5');


select * from bdar.postgres_log pl
select count(*) from bdar.postgres_log pl
select from bdar.postgres_log pl;


copy tmp_table from program 'cat /var/lib/postgresql/12/main/log/*.csv' with csv ;

select  setting from pg_catalog.pg_settings where name = 'data_directory'
select * from pg_tablespace;
select * from bdar.postgres_log pl;
delete from bdar.postgres_log
create view persons_view as 
select aa.id, aa.firstname, aa.lastname, aa.email,aa.phone_number, ad."name",ad.address1, ad.address2, ad.city, ad.state_province, ad.postal_code from account aa
left join address ad on ad.account_id = aa.id;

select * from account_full am



drop view account_master;
create extension if not exists anon cascade;
drop extension anon;
SECURITY LABEL FOR anon ON COLUMN account_full.firstname IS 'MASKED WITH FUNCTION anon.fake_first_name()';

SELECT anon.anonymize_table('account_full');

select * from pg_catalog.pg_available_extensions pae

create extension if not exists pgcrypto

update credit_card set cc_num = (select PGP_SYM_ENCRYPT('1000 1234 5678 9010', 'AES_KEY') as cc_num) where id = 1;
select * from credit_card cc

select PGP_SYM_DECRYPT('\xc30d040903025edebe1fd068d02565d2390124c1b5ea5fa4b2ccce0a2d55b2c55e3fd3d0429223e4c56474f509eb38b78d833f9602508c46dcecac32221aff470ae019cac0f3e935abea', 'AES_KEY')







Anonymization lygiai lenteleje - Lygis, value kuria naudos, komanda

Funkcija kuri anonimizuoja gauna ( duomenis selecto pavidalu, masyva kiekvieno stulpelio anonimizavimo komandai tai tarkim [null, zip, null, phone ir .tt], Lygi)
https://stackoverflow.com/questions/24881926/pass-a-select-result-as-an-argument-to-postgresql-function

Funkcija sukuria viewa, i kuri ikelia visus selectus  pvz :
 CREATE VIEW anonymized_patient AS
SELECT 
    'REDACTED' AS name,
    anon.generalize_int4range(zipcode,100) AS zipcode,
    anon.generalize_tsrange(birth,'decade') AS birth
    disease
FROM patients;

ir kiekviena stulepi uzmaskuoja.

Tada dar reiks funkciju, savo anonimizavimo lygio kurimui, kur bus paduodamos reiksmes - lygio pavadinimas, ir is eiles parametrai kiekvienai komandai, reiksmes ir kt.
Dar reiks nepamirsk k anonimity funkcijOS checko is anonym extensiono gal kazkaip prideti i mano extensiona






select remove_anon_rule('phone','SUPER')
select add_anon_rule('phone',array['0','xxxx','0'], 'SUPER')

select * from bdar_tables.anon_config ac;

select  bdar_anonimyze('HIGH', 'persons_view', array[null,null,null,'email','phone',null,null,null,null,null,'zip'])
select bdar_anonimyze_column('HIGH', 'persons_view', 'phone_number', 'phone');

select * from persons_view_anonimyzed pva2

select * from bdar_tables.anon_config ac
select * from account_view_anonimyzed ava

select c.column_name
       from information_schema.tables t
    left join information_schema.columns c 
              on t.table_schema = c.table_schema 
              and t.table_name = c.table_name
where table_type = 'VIEW' 
      and t.table_schema not in ('information_schema', 'pg_catalog', 'anon')
      and t.table_name = 'account_view'
SELECT anon.load();

select anon.partial('abcdefghadsa',1,'****',3)
      
    
create or replace view persons_view as select * from acc;

create or replace VIEW account_view_generalized AS
SELECT 
	id,
	firstname,
	lastname,
	email,
	anon.partial(phone_number,4,****,2) as phone_number,
	name,
	address1,
	address2,
	city,
	state_province,
	anon.generalize_int4range(postal_code::INTEGER,100) AS postal_code	
FROM account_view;

drop view account_view_generalized;
select * from account_view_generalized avg2;
----------------------------------------------------------------------------------------------------------------------------

reikes lenteles kuri turis nuoroda i stulpeli kuri reikia kriptuot
kriptavimo raktas gali guleti kazkur kompe kazkokima faile, ir reikes nurodyti funkcijai patha iki to failo su raktu, taip bus nuskaitomas raktas
reikia, kad extensionas sukurtu kazkoki trigeri kai yra pridedamas jautrus stulpelis, kad darant inserta tai lentelei jis suveiktu, ir uzmaskuotu ta lauka.
toks pat trigeris atvirkstiniam mechanizmui kai reikia duomenis pasiimti. 

Patikrinti, kas bus jei raktas negeras?


create table crypted_columns(
	table_schema varchar,
	table_name varchar,
	table_column varchar
);

create extension hstore;

CREATE or replace FUNCTION encrypt_column() 
  RETURNS trigger AS
$$
declare
   v_sql text;
   v_sql1 text;
  _col text := quote_ident(TG_ARGV[0]);
begin
   execute 'select PGP_SYM_ENCRYPT('||'$1'||'.'||TG_ARGV[0]||', ''AES_KEY'')' using new into v_sql;
   NEW:= NEW #= hstore(_col, v_sql);  
   RETURN NEW;
END;
$$
language plpgsql;

CREATE or replace FUNCTION decrypt_column() 
  RETURNS trigger AS
$$
declare
   v_sql text;
   v_sql1 text;
  _col text := quote_ident(TG_ARGV[0]);
begin
   execute 'select PGP_SYM_ENCRYPT('||'$1'||'.'||TG_ARGV[0]||', ''AES_KEY'')' using new into v_sql;
   NEW:= NEW #= hstore(_col, v_sql);  
   RETURN NEW;
END;
$$
language plpgsql;





---------------------
select cc_num, PGP_SYM_DECRYPT(cc_num::bytea, 'AES_KEY') from credit_card cc;







create trigger test_trigger before insert on credit_card for each row execute procedure crypt_column('cc_num');

drop trigger test_trigger on credit_card;


select * from credit_card cc

delete from credit_card cc

select * from bdar_show_activity_log();
select * from bdar_tables.conf c

