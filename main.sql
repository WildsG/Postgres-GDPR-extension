-- "RELATIONAL DATABASE SCHEMA ESHOP" GOOGLE THIS FOR RELATIONAL DB EXAMPLES."TEEST
create schema bdar;
set search_path to bdar;
create table account(
   id serial primary key,
   username varchar (50) unique not null,
   password_hash varchar not null,
   firstname varchar(50) not null,
   lastname varchar(100) not null,
   email varchar (355) unique not null,
   phone_number varchar(15) unique not null,
   created_on timestamp not null,
   last_login timestamp
);

create table credit_card(
   id serial primary key,
   cc varchar unique not null,
   cc_num varchar not null,
   holder_name varchar not null,
   expire_date timestamp not null,
   account_id integer references account(id)
);

create table product(
   id serial primary key,
   product_name varchar(100) not null,
   product_type varchar(20),
   description varchar(255),
   stock integer not null,
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
	purchase_date timestamp not null
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

-- iterpiami testinius duomenis i testini duomenu modelis

insert into account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('vardenis', md5('password'),'vardenis', 'pavardenis' ,'vardenis.test@gmail.com','+37000000000', current_timestamp,current_timestamp + (20 * interval '1 minute'));

insert into account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('vardelis', md5('password'),'vardelis', 'pavardelis' ,'vardelis.test@gmail.com','+3701234567', current_timestamp,current_timestamp + (20 * interval '1 minute'));

insert into account(username,password_hash,firstname, lastname,email,phone_number,created_on,last_login) values 
('vardauskas', md5('password'),'vardauskas', 'pavardauskas' ,'vardauskas.test@gmail.com','+3709876652', current_timestamp,current_timestamp + (20 * interval '1 minute'));

insert into credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
('1000 1234 5678 9010','111','vardenis pavardenis', current_timestamp + (5 * interval '1 year'),1);

insert into credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
('1000 1234 5678 9011','112','vardenis pavardenis', current_timestamp + (5 * interval '1 year'),1);

insert into product(product_name, product_type, description, stock, price) values 
('samsung galaxy 7', 'smartphone', 'good, reliable, but weak battery', 1, 80);
insert into product(product_name, product_type, description, stock, price) values 
('samsung galaxy 8', 'smartphone', 'better than 7 , good, reliable, but weak battery', 1, 180);
insert into product(product_name, product_type, description, stock, price) values 
('samsung galaxy 9', 'smartphone', 'better than 8 , good, reliable, but weak battery', 1, 380);

insert into review(account_id, product_id,message, rating) values 
(1, 2, 'i liked it', 9);
insert into review(account_id, product_id,message, rating) values 
(1, 1, 'not so good. quite old', 7);

insert into purchase_history(account_id, product_id, purchase_date) values
(1,2, current_timestamp);
insert into purchase_history(account_id, product_id, purchase_date) values
(1,1, current_timestamp);


insert into address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('home', 'konstitucijos 00-0', null, 'vilnius', 'vilniaus apskritis', '45678', 1);
insert into address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('home', 'konstitucijos 00-0', null, 'vilnius', 'vilniaus apskritis', '44009', 2);
insert into address(name, address1, address2, city ,state_province, postal_code, account_id) values 
('home', 'konstitucijos 00-0', null, 'vilnius', 'vilniaus apskritis', '46888', 3);

-- SUKURIAME VAIZDA, KURIS VELIAU BUS NAUDOJAMAS ANONIMIZAVIMUI
CREATE VIEW PERSONS_VIEW AS 
SELECT AA.ID, AA.FIRSTNAME, AA.LASTNAME, AA.EMAIL,AA.PHONE_NUMBER, AD."NAME",AD.ADDRESS1, AD.ADDRESS2, AD.CITY, AD.STATE_PROVINCE, AD.POSTAL_CODE FROM ACCOUNT AA
LEFT JOIN ADDRESS AD ON AD.ACCOUNT_ID = AA.ID;

drop extension pg_cron
drop extension bdar_helper
-- PATIKRINAME LENTELESE ESANCIUS DUOMENIS
select * from account;
select * from credit_card;
select * from product;
select * from review;
select * from purchase_history;
select * from address;
select * from persons_view pv


-- sudiegiami visus pletinius nuo kuriu priklauso sis bdar pletinys
create extension pg_cron;
create extension if not exists anon cascade;
create extension if not exists pgcrypto
create extension hstore;
create extension bdar_helper;

-- patikriname ir iterpiame privacius duomenis saugancias lenteles
select * from bdar_tables.private_entities

select add_private_entity('bdar', 'address');
select add_private_entity('bdar', 'credit_card');
select add_private_entity('bdar', 'review');
select add_private_entity('bdar', 'purchase_history');
select * from bdar_tables.private_entities

--- pletinio funkcijos vartotojo uzmirsimui
select bdar_forget('bdar', 'account', '1');
select bdar_forget_configured('bdar', 'account', '1');

--- patikrinimo selectai, kuriu pagalba galima pasiziureti kaip uzsipilde pletinio lenteles
select * from bdar_tables.delayed_delete_rows
select * from bdar_tables.cron_log

--- funkcija pasiziureti vartotoju veiksmu loga
select * from bdar_show_activity_log();

--- pletinio lentele konfiguracijoms
select * from bdar_tables.conf
--- atnaujiname parametro reiksme
select update_parameter('delete_wait_time_minutes', '5');

--- pridedama anonimizavimo taiskykles
select add_anon_rule('phone',array['0','xxxx','0'], 'SUPER')
--- patikriname esamas anonimizavimo taisykles
select * from bdar_tables.anon_config ac;
--- anonimizuojame vaizda
select  bdar_anonimyze('HIGH', 'persons_view', array[null,null,null,'email','phone',null,null,null,null,null,'zip'])
--- anonimizuojama vaizdo stuleplis
select bdar_anonimyze_column('HIGH', 'persons_view', 'phone_number', 'phone');
--- patikrinamas anonimizuotas vaizdas
select * from persons_view_anonimyzed pva
--- pasaliname anonimizavimo taisykle
select remove_anon_rule('phone','SUPER')

--- nustatome sifruojama stulpeli
select set_crypted_column('bdar','credit_card','cc', 'mykey')
--- iterpiame nauja irasa i sifruojama lentele
insert into credit_card(cc,cc_num,holder_name,expire_date,account_id) values 
('1000 1234 5678 1111','222','varde pavarde', current_timestamp + (5 * interval '1 year'),2);
--- patikrinam kaip atrodo sifruotas irasas
select * from credit_card cc2
--- issifurojam iraso informacija
select cc, pgp_sym_decrypt(cc::bytea, 'mykey') from credit_card ;
--- pasaliname kriptuojamo stulpelio tirggeri
select remove_crypted_column('bdar','credit_card','cc')


