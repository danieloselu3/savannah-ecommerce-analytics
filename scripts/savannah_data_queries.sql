-- Joining Users and Carts
CREATE OR REPLACE TABLE `savannah-info-analytics-001.ecommerce_data.enr_user_carts` AS
SELECT 
  u.user_id, 
  u.first_name, 
  u.last_name, 
  u.gender, 
  u.age, 
  u.city, 
  u.postal_code, 
  c.cart_id, 
  c.product_id, 
  c.quantity, 
  c.price, 
  c.total_cart_value
FROM `savannah-info-analytics-001.ecommerce_data.users_table` u
JOIN `savannah-info-analytics-001.ecommerce_data.carts_table` c 
ON u.user_id = c.user_id
ORDER BY c.cart_id ASC
;

-- Joining Carts and Products
CREATE OR REPLACE TABLE `savannah-info-analytics-001.ecommerce_data.enr_cart_products` AS
SELECT 
  c.cart_id, 
  c.user_id, 
  c.product_id, 
  p.name AS product_name, 
  p.category, 
  p.brand, 
  c.quantity, 
  c.price, 
  c.total_cart_value
FROM `savannah-info-analytics-001.ecommerce_data.carts_table` c
LEFT JOIN `savannah-info-analytics-001.ecommerce_data.products_table` p 
ON c.product_id = p.product_id
ORDER BY c.cart_id ASC
;

-- User Summary
CREATE OR REPLACE TABLE `savannah-info-analytics-001.ecommerce_data.agg_user_summary` AS
SELECT 
  user_id, 
  first_name, 
  ROUND(SUM(total_cart_value),2) AS total_spent, 
  SUM(quantity) AS total_items, 
  age, 
  city
FROM `savannah-info-analytics-001.ecommerce_data.enr_user_carts` c
GROUP BY user_id, first_name, age, city
ORDER BY total_spent DESC
;


-- Category Summary
CREATE OR REPLACE TABLE `savannah-info-analytics-001.ecommerce_data.agg_category_summary` AS
SELECT 
  category, 
  ROUND(SUM(total_cart_value),2) AS total_sales, 
  SUM(quantity) AS total_items_sold
FROM `savannah-info-analytics-001.ecommerce_data.enr_cart_products`
GROUP BY category
ORDER BY total_sales DESC;


-- Cart Details
CREATE OR REPLACE TABLE `savannah-info-analytics-001.ecommerce_data.enr_cart_details` AS
SELECT 
  uc.*, 
  p.name AS product_name, 
  p.category, 
  p.brand, 
  p.price AS product_price
FROM `savannah-info-analytics-001.ecommerce_data.enr_user_carts` uc
LEFT JOIN `savannah-info-analytics-001.ecommerce_data.products_table` p
ON uc.product_id = p.product_id;

