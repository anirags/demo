-- section 1 : Date: 28-04-23
-- first challenge to fetch first_name, last_name, email of customer 
select first_name, last_name, email from customer

-- order by clause
SELECT * FROM payment ORDER BY customer_id , amount DESC

-- 2nd challenge to fetch orderd customers list in Z to A order
-- in case same last name order first name also in Z to A order.
SELECT first_name, last_name, email
FROM customer ORDER BY  last_name DESC,first_name DESC

-- distinct 
SELECT DISTINCT rating, count(rating) as countr 
FROM film GROUP BY rating ORDER BY countr desc

-- 3rd challenge to fetch diffrent amount paid by customer in order high to low.
SELECT DISTINCT amount FROM payment ORDER BY amount DESC

-- 28-04-23 section 1 challenge.
-- 1st. create a list of all distinct districts customer are from
SELECT DISTINCT district FROM address

--2nd. fetch lateast rental date
SELECT rental_date FROM rental ORDER BY rental_date DESC LIMIT 1 

--3rd. How many film company has.
SELECT COUNT(film_id) FROM film

--4th. How many customer distinct last name are there.
SELECT COUNT(DISTINCT last_name) FROM customer

--challenge is how many payment were made by customer with customer_id = 100
SELECT count(*) payment_made FROM payment WHERE customer_id = 100

-- challenge is what is last name of customer with first name = 'ERICA'
SELECT first_name,last_name FROM customer WHERE first_name = 'ERICA'

-- challenge is how many rental have not been returned yet(return date is null)
SELECT COUNT(*) FROM rental WHERE return_date is null

-- challenge is list of all payment id with amount is less than or equal to $2
-- includes payment id and amount.
SELECT payment_id , amount FROM payment WHERE amount <= 2 

-- challenge is list records with customer id 322, 346 and 354 where amount
-- should be either less than 2 or greater than 10.
SELECT * FROM payment where (customer_id = 322 OR
customer_id = 346 OR customer_id = 354) AND (amount < 2 OR amount > 10)
ORDER BY customer_id ASC, amount DESC

-- how many payment have been made on jan 26th and 27th 2020 with an amount 
-- between 1.99 and 3.99
SELECT COUNT(*) FROM payment WHERE payment_date 
between '2020-01-26' AND '2020-01-27 23:00' 
AND amount between 1.99 and 3.99

SELECT payment_id FROM payment WHERE customer_id  
IN(12, 25, 67,93, 124, 234) AND amount IN (4.99, 7.99, 9.99) 
AND payment_date between '2020-01-01' AND '2020-01-31 23:59' 

-- challenge is how many customer are there with first name 
--having 3 letter long and  in last name having eigther 'X' or 'Y' 
-- as last letter.
SELECT COUNT(*) FROM customer WHERE first_name LIKE '___'
AND (last_name LIKE '%X' OR last_name LIKe '%Y')

/*section 2 challenge 28-04-23
1st. how many movie are there that contain 'Saga' in their discription 
and their title start eigther with 'A' or ends with 'R' ? */
SELECT COUNT(*) FROM film WHERE description LIKE '%Saga%' AND 
(title LIKE 'A%' OR title LIKE '%R')

/*2nd. create a list of all customers who cotanins 'ER' in first name and 
has 'A' as second letter order by last name desc*/
SELECT * FROM customer WHERE first_name LIKE '%ER%' AND 
first_name LIKE '_A%' ORDER BY last_name DESC

-- Section 3 Date: 01-05-23
/*challenge is thatwhich of two employees are responsible for more payments */
--SELECT * FROM payment 
SELECT staff_id, COUNT(payment_id) FROM payment 
GROUP BY staff_id ORDER BY COUNT(payment_id) DESC LIMIT 2

/*challenge is that which of two employees are responsible for higher overall payment
amount*/
SELECT staff_id, SUM(amount) FROM payment 
GROUP BY staff_id ORDER BY SUM(amount) DESC LIMIT 2

/*challenge is that which employee had the highest sales amount 
in a single day*/
SELECT staff_id, SUM(amount), DATE(payment_date) FROM payment 
GROUP BY staff_id, DATE(payment_date) ORDER BY SUM(amount) DESC 

/*challenge is that which employee had the most sales in a single day
(not count the payment with amount 0)*/
SELECT staff_id, COUNT(payment_id), DATE(payment_date) FROM payment 
WHERE amount <> 0
GROUP BY staff_id, DATE(payment_date) ORDER BY COUNT(payment_id) DESC

/*date should be 2020 april 28, 29 and 30
Find out the average payment amount grouped by customer and day, 
only day/customers with more than 1 payment 
order by average amount in desc order*/

SELECT customer_id, DATE(payment_date), ROUND(AVG(amount), 2), COUNT(payment_id)
FROM payment WHERE DATE(payment_date) 
in ('2020-04-28','2020-04-29','2020-04-30')
GROUP BY customer_id, DATE(payment_date) HAVING COUNT(payment_id) > 1
ORDER BY AVG(amount) DESC

-- section 4 
/*challenge is to create anonymous email like a**.b**@exgmail.com and **l.**i@exgmail.com*/
SELECT * FROM staff;
SELECT LEFT(email,1) || '***' || '.' || LEFT(SUBSTRING(email FROM POSITION('.' IN email)+1 for
POSITION('@' IN email)-POSITION('.' IN email) ),1) || '***' || SUBSTRING(email from POSITION('@' IN email))
FROM staff 
SELECT LEFT(email,1) || '***' || SUBSTRING(email from POSITION('@' IN email))
FROM staff 

--section 5 Date: 02-05-23
SELECT customer_id, count(payment_id),
case 
when count(payment_id) > 30 then 'targeted customer'
else 'not a targeted customer'
end
from payment group by customer_id
order by count(*) DESC

-- challenge is list customer with tag high, mid and low based on total payment they make
select customer_id, count(customer_id),
case 
when sum(amount) < 150 then 'Low'
when sum(amount) > 150 or sum(amount) < 200 then 'mid'
when sum(amount) > 200 then 'high'
end as tag
from payment group by customer_id

-- challenge using coalasce and cast
select rental_date, coalesce(cast(return_date as varchar), 'Not returned') 
from rental where return_date is null

--section 6 join 
/*The company want to run a phone call campaign to list all customer of texas (= district)
fields includs first_name, last_name , phone no , district*/
select * from address
select * from customer
select c.first_name, c.last_name, a.phone , a.district 
from address a inner join customer c 
on a.address_id = c.address_id
where a.district = 'Texas'

/*Are there any addresses that are not related to any customer*/
select *
from address a left join customer c 
on a.address_id = c.address_id
where c.customer_id is null

-- Section 7 Date: 03-05-23
/*challenge is that select all of the films where length is more than avg or all films*/
select film_id, title from film
where length > (select avg(length) from film)

/*Return all of the films that are available in the inventory in store 2 more than 3 times*/
select film_id, title from film
where film_id in 
(select film_id from inventory where store_id = 2 group by film_id having count(*) > 3 ) 

/*Return all customers's first name and last name that have made a payment on '2020-01-25'*/
select first_name, last_name from customer 
where customer_id in 
(select customer_id from payment where date(payment_date) = '2020-01-25')

/*Return all customers's first name and email addresss that have spent more than $30 */
select first_name, email from customer 
where customer_id in 
(select customer_id from payment group by customer_id having sum(amount) > 30)

/*Return all customers's first name and last name that that are from california 
and spent more than $100 in total*/
select first_name, email from customer 
where customer_id in (select customer_id from address a
inner join customer c on a.address_id = c.address_id 
where district = 'California') and customer_id in 
(select customer_id from payment group by customer_id having sum(amount) > 100) 

-- challenge 
select round (avg(avg_amt),2) from
(select sum(amount)as avg_amt, date(payment_date) from payment 
group by date(payment_date)) as a

--section 8 full challenges
/*1st. Task: Create a list of all the different (distinct) replacement costs of the films.
Question: What's the lowest replacement cost?*/

select min(distinct(replacement_cost)) from film 

/*2.Task: Write a query that gives an overview of how many films 
have replacements costs in the following cost ranges.
low: 9.99 - 19.99
medium: 20.00 - 24.99
high: 25.00 - 29.99
Question: How many films have a replacement cost in the "low" group?*/

select 
case 
when replacement_cost between 9.99 and 19.99 then 'low'
when replacement_cost between 20.00 and 24.99 then 'mid'
else 'high'
end as cost_range, count(*)
from film 
group by cost_range 

/*3. Task: Create a list of the film titles including their title, length, and category name ordered 
descendingly by length. Filter the results to only the movies in the category 'Drama' or 'Sports'.
Question: In which category is the longest film and how long is it?*/

select title, length, c.name from film f inner join film_category fc
on f.film_id = fc.film_id inner join category c on fc.category_id = c.category_id 
where c.name in('Drama', 'Sports')
order by length desc

/*4.Task: Create an overview of the actors' first and last names and in how many movies they appear in.
Question: Which actor is part of most movies??*/

select a.first_name, a.last_name , count(*) from actor as a
inner join film_actor as fs on a.actor_id = fs.actor_id
group by a.first_name, a.last_name 
order by count(*) desc

/*5.Task: Create an overview of the cities and how much sales (sum of amount) have occurred there.

Question: Which city has the most sales?*/
select city, sum(amount) from  payment p left join customer c 
on p.customer_id = c.customer_id left join address a
on a.address_id = c.address_id  left join city ct
on a.city_id = ct.city_id 
group by city
order by sum(amount) desc

/*6. Task: Create an overview of the addresses that are not associated to any customer.
Question: How many addresses are that?*/

select customer_id,first_name, last_name , address from customer c left join address a
on c.address_id = a.address_id
where first_name is null

/*Create an overview of the revenue (sum of amount) grouped by a column in the format "country, city".
Question: Which country, city has the least sales?*/
-- payment -> customer -> address -> city -> country
select sum(amount), country, city from payment p left join customer c 
on p.customer_id = c.customer_id left join address a 
on c.address_id = a.address_id left join city ct 
on a.city_id = ct.city_id left join country ctry 
on ct.country_id = ctry.country_id 
group by country , city 
order by sum(amount) asc

/*Task: Create a list with the average of the sales amount each staff_id has per customer.
Question: Which staff_id makes on average more revenue per customer?*/

select staff_id, round(avg(avg_sum),2) from
(select customer_id ,staff_id ,sum(amount) as avg_sum 
from payment group by customer_id,staff_id order by staff_id ) a group by staff_id

/*Create a query that shows average daily revenue of all Sundays.
Question: What is the daily average revenue of all Sundays?*/

select round(avg(total),2) from (select sum(amount)as total, date(payment_date) ,  
extract(dow from payment_date ) as weekday from payment where extract(dow from payment_date ) = 1
group by date(payment_date), weekday ) daily

--section 9 
-- create table 
create table director
(director_id serial primary key,
director_account_name varchar (20) unique ,
first_name varchar(50),
last_name varchar(50),
address_id int references address(address_id)
)

alter table director 
alter column last_name set not null,
alter column director_account_name type varchar(30),
add column email varchar(40)
select * from director

alter table director rename column director_account_name to account_name

-- Create table
CREATE TABLE emp_table 
(
	emp_id SERIAL PRIMARY KEY,
	emp_name TEXT
)

-- SELECT table
SELECT * FROM emp_table

-- Drop table
drop table emp_table

-- Insert rows
INSERT INTO emp_table
VALUES
(1,'Frank'),
(2,'Maria')

-- SELECT table
SELECT * FROM emp_table

-- Truncate table
truncate emp_table

-- challenge to create a table with some constraints 
create table songs 
(song_id serial primary key , song_name varchar(30) not null, 
genre varchar(30) default 'NOt defined'
, price numeric(4,2) check (price >= 1.99),
 release_date date constraint date_check check ( release_date between'01-01-1950' and current_date )  )
 
 select * from songs
 
 insert into songs (song_name, price, release_date)
 values('SQL song', 0.99, '01-07-2022')
 truncate songs
 alter table songs 
 drop constraint songs_date_check --check ( release_date between current_date and '01-01-1950') 
 
 
 select * from customer
 
 update customer 
 set initials = left(first_name,1) ||'.' || left(last_name,1) ||'.'
 
 -- over partition example 
 SELECT *, sum(amount) OVER(PARTITION BY customer_id ) from payment
 
 SELECT *, sum(amount) OVER(ORDER BY payment_date ) from payment
 
 --roll up
 
 select 'Q'||TO_CHAR(payment_date, 'Q') as quarter,
 extract(month from payment_date) as month,
 to_char(payment_date,'W')as week_of_month,
 date(payment_date),
 sum(amount)
 from payment
 group by 
 rollup('Q'||TO_CHAR(payment_date, 'Q') ,
 extract(month from payment_date),
to_char(payment_date,'W'),
 date(payment_date))
 order by 1,2,3,4
 
/*Write a query that returns all grouping sets in all combinations 
of customer_id, date and title with the aggregation of the payment amount.*/
SELECT 
p.customer_id,
DATE(payment_date),
title,
SUM(amount) as total
FROM payment p
LEFT JOIN rental r
ON r.rental_id=p.rental_id
LEFT JOIN inventory i
ON i.inventory_id=r.inventory_id
LEFT JOIN film f
ON f.film_id=i.film_id
GROUP BY 
CUBE(
p.customer_id,
DATE(payment_date),
title
)
ORDER BY 1,2,3

--self join 
CREATE TABLE employee (
	employee_id INT,
	name VARCHAR (50),
	manager_id INT
);

INSERT INTO employee 
VALUES
	(1, 'Liam Smith', NULL),
	(2, 'Oliver Brown', 1),
	(3, 'Elijah Jones', 1),
	(4, 'William Miller', 1),
	(5, 'James Davis', 2),
	(6, 'Olivia Hernandez', 2),
	(7, 'Emma Lopez', 2),
	(8, 'Sophia Andersen', 2),
	(9, 'Mia Lee', 3),
	(10, 'Ava Robinson', 3);

select e1.employee_id, 
e1.name as emp_name,e2.name as mng_name , e2.manager_id , e3.name as mgn_of_mng from employee e1 left join employee e2 
on e1.employee_id = e2.manager_id 
left join employee e3 on e3.manager_id = e2.manager_id