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

create view persons_view as 
select aa.id, aa.firstname, aa.lastname, aa.email,aa.phone_number, ad."name",ad.address1, ad.address2, ad.city, ad.state_province, ad.postal_code from account aa
left join address ad on ad.account_id = aa.id;


select * from account;
select * from credit_card;
select * from product;
select * from review;
select * from purchase_history;
select * from address;
select * from persons_view pv