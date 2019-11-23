-- "relational database schema eshop" google this for relational db examples."

CREATE SCHEMA bdar;

CREATE TABLE account(
   id serial PRIMARY KEY,
   username VARCHAR (50) UNIQUE NOT NULL,
   password_hash VARCHAR NOT NULL,
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


INSERT INTO account(username,password_hash,email,phone_number,created_on,last_login) values 
('Arturas', MD5('password'), 'arturas.test@gmail.com','+37000000000', CURRENT_TIMESTAMP,current_timestamp + (20 * interval '1 minute'));

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



select * from account;
select * from credit_card;
select * from product;
select * from review;
select * from purchase_history;



select * from pg_available_extensions;

create extension base36;