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


 
