USE DataWarehouse;

------------------------------- Change over time trends ( Analyse how measure evolves over time) --------------------------------------

select 
year(order_date) as order_year,
month(order_date) as order_month,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by year(order_date),month(order_date)
order by year(order_date),month(order_date);

select 
DATETRUNC(month,order_date) as order_date ,--------------- rounds a date or timestamp to a specified date part - month or year
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
order by DATETRUNC(month,order_date);

select 
format(order_date,'yyyy-MMM') as order_date ,--------------- format the date and the output will be string it wont sort properly like datetrunc
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by format(order_date,'yyyy-MMM')
order by format(order_date,'yyyy-MMM');

------------------------------- Cumulative analysis (aggregate the data progressively over time ) --------------------------------------

-- total sales of each month and running total of sales over time

select
order_date,
total_sales,
sum(total_sales) over(partition by order_date order by order_date) as running_total
from
(
select 
DATETRUNC(MONTH,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(MONTH,order_date)
)t

-- moving avgerage of the price

select
order_date,
avg_price,
total_sales,
sum(total_sales) over(order by order_date) as running_total_sales,
avg(avg_price) over(order by order_date) as moving_avgerage_price
from
(
select 
DATETRUNC(YEAR,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(YEAR,order_date)
)t

------------------------------- Performance analysis (Comparing the current value to a target value ) --------------------------------------

 -- yearly performance of products -- comparing average sales and previous year sales
 with yearly_product_sales as (
 select  
 year(s.order_date) as order_year,
 p.product_name,
 sum(s.sales_amount) as current_sales
 from gold.fact_sales s
 left join gold.dim_products p
 on s.product_key =	p.product_key
 where s.order_date is not null
 group by year(s.order_date),product_name)

 select 
 order_year,
 product_name,
 current_sales,
 avg(current_sales) over ( partition by product_name) as avg_sales,
 current_sales -  avg(current_sales) over ( partition by product_name) as diff_sales_avg,
 case when  current_sales -  avg(current_sales) over ( partition by product_name) >0 then 'Above Avg'
	  when  current_sales -  avg(current_sales) over ( partition by product_name) <0 then 'Below Avg'
	  else 'Avg'
 end as avg_change, ----  average sales

 Lag(current_sales) over (partition by product_name order by order_year) as prev_sales,
 current_sales -  Lag(current_sales) over (partition by product_name order by order_year) as diff_sales_prev,
 case when current_sales -  Lag(current_sales) over (partition by product_name order by order_year) >0 then 'Increse'
	  when  current_sales -  Lag(current_sales) over (partition by product_name order by order_year) <0 then 'Decrease'
	  else 'No Change'
 end as prev_change ----  previous year sales

 from yearly_product_sales
 order by 2,1

 ------------------------------- part to whole analysis (how an individual part is performing compared to overall) --------------------------------------

 -- which categories contribute the most to overall sales
 with cat_sales as (
 Select
 category,
 sum(sales_amount) as total_sales
 from gold.fact_sales s
 left join gold.dim_products p
 on s.product_key =	p.product_key
 group by category )

 Select 
 category,
 total_sales,
 sum(total_sales) over() as overall_sales,
 concat(round((cast(total_sales as float)/sum(total_sales) over() )*100,2),'%') as cat_per
 from cat_sales
 order by total_sales desc

  ------------------------------- Data Segmentation (Group the data based on a specific range) --------------------------------------

  -- segment products into cost ranges and count -products fall into each segment

  with cost_segment as(
  select 
  product_key,
  product_name,
  product_cost,
  case when product_cost <100 then 'Below 100'
       when product_cost between 100 and 500 then '100-500'
	   when product_cost between 500 and 1000 then '500-1000'
	   else 'Above 1000'
  end as cost_range

  from gold.dim_products)

  select cost_range ,count(product_key) as total_products 
  from cost_segment
  group by cost_range
  order by total_products desc


  -- Group customers based on spending behavior 
  --				vip ( atleast 12 months of history and spending more than 5000)
  --				regular ( atleast 12 months of history and spending <= 5000)
  --				new ( lifespan less than 12 months)

  with customer_spending as(
  select 
  c.customer_key,
  sum(s.sales_amount) as total_spend,
  min(s.order_date) as first_order,
  max(s.order_date) as last_order,
  datediff(month,min(s.order_date), max(s.order_date)) as lifespan
  from gold.fact_sales s
  left join gold.dim_customers c
  on s.customer_key = c.customer_key
  group by c.customer_key
  )

  select customer_segment ,count(customer_key) as cnt
  from (
  select 
  customer_key,
  case when total_spend > 5000 and lifespan >= 12 then 'VIP'
       when total_spend <= 5000 and lifespan >= 12 then 'Regular'
	   Else 'New'
  end as customer_segment
       
  from customer_spending)t
  group by customer_segment
  order by count(customer_segment) desc




