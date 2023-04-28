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