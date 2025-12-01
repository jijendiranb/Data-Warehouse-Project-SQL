USE DataWarehouse;


SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON
s.product_key = p.product_key
LEFT JOIN gold.dim_customers c ON
s.customer_key = c.customer_key;

------- Database Exploration ( knowing about structure of tables and views) ------------------

-- Explore all objects in th datebase

SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- Explore all columns in the database

SELECT * FROM INFORMATION_SCHEMA.COLUMNS;

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales';

------- Dimension Exploration ( identifying the unique values(or categories) in each dimension ) (low and hig cardinality dimension - based on unique values) ----------------------

-- Explore all countries our customers come from.

SELECT Distinct country from gold.dim_customers;

-- Explore all categories "The Major Divisions"

SELECT Distinct category,subcategory,product_name from gold.dim_products
ORDER BY 1,2,3;

--------- Date Exploration ( identify the earliest and latest dates (boundaries). -----------------------------------

--- find the date of the first and last order

SELECT MIN(order_date) first_order_date,
MAX(order_date) last_order_date ,
DATEDIFF (year,MIN(order_date),MAX(order_date)) as no_of_yearsinsales
FROM gold.fact_sales; --- we have 4 years of sales in business

-- find the youngest and older customer

SELECT MIN(birthdate) youngest,
DATEDIFF(year,MIN(birthdate),GETDATE()) oldest_age,
MAX(birthdate) oldest,
DATEDIFF(year,max(birthdate),GETDATE()) youngest_age
FROM gold.dim_customers;

--------- Measures Exploration (Key metrics of the business). -----------------------------------

--- Find the total sales
Select sum(sales_amount) as total_sales from gold.fact_sales;

--- how many items are sold
Select sum(quantity) as total_quantity from gold.fact_sales;

--- average selling price
Select AVG(price) as avg_price from gold.fact_sales;

--- number of orders,products and customers
Select count(order_number) as total_orders from gold.fact_sales;
Select count(distinct order_number) as total_orders from gold.fact_sales;

Select count(product_key) as total_products from gold.dim_products;
Select count(distinct product_key) as total_products from gold.dim_products;

Select count(customer_key) as total_products from gold.dim_customers;
Select count(distinct customer_key) as total_products from gold.dim_customers;

--- customers who placed an order
Select count(distinct customer_key) as total_products from gold.fact_sales;

---- generating a report that shows all key metrics of the business
select 'Total Sales' as measure_name , sum(sales_amount) as measure_value from gold.fact_sales
union all
select 'Total Quantity',sum(quantity) from gold.fact_sales
union all
select 'Average Price', AVG(price)  from gold.fact_sales
union all
select 'Total Nr. Orders',count(distinct order_number)  from gold.fact_sales
union all
select 'Total Nr.Products',count(distinct product_key)  from gold.dim_products
union all
select 'Total Nr. Customers',count(distinct customer_key) from gold.dim_customers;


--------- Magnitude Analysis (compare the measure values by categries-- measure by dimension). -----------------------------------

-- total customers by country , gender
select country,gender,count(customer_key) as total_customers from gold.dim_customers
group by country,gender
order by count(customer_key) desc ;


-- toatl products by category
select category,count(product_key) as total_products from gold.dim_products
group by category
order by total_products desc;

-- avg cost in each category
select category,avg(product_cost) as avg_cost from gold.dim_products
group by category
order by avg_cost desc;

-- revenue from each category
select p.category ,sum(f.sales_amount) as total_revenue  from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category 
order by total_revenue desc;

-- revenue by each cutomer
select c.customer_key,c.first_name,c.last_name ,sum(f.sales_amount) as total_revenue  from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key,c.first_name,c.last_name 
order by total_revenue desc;

-- distribution of sold items across countries
select c.country, sum(f.quantity) as total_quantity  from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.country
order by total_quantity desc;

--------- Ranking Analysis (order the values of dimensions by measure). -------------------------------------------------

 -- top 5 products - highest revenue
SELECT top 5 
 p.product_name,
 SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p 
ON p.product_key = f.product_key
GROUP BY p.product_name
order by total_revenue desc;

 -- worst 5 products
SELECT  top 5
 p.product_name,
 SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p 
ON p.product_key = f.product_key
GROUP BY p.product_name
order by total_revenue ASC;

-- customer with fewer orders
select top 3
c.customer_key,c.first_name,c.last_name ,
count(distinct f.order_number) as total_orders  
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key,c.first_name,c.last_name 
order by total_orders ASC;

-- top customers
select top 10
c.customer_key,c.first_name,c.last_name ,
SUM(f.sales_amount) AS total_revenue 
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key,c.first_name,c.last_name 
order by total_revenue desc;