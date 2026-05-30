-- Sample Cosmos DB SQL queries
-- These can be run in the Azure Portal Data Explorer for the Cosmos DB container

-- 1. Get all transactions
SELECT * FROM c

-- 2. Get transactions by customer
SELECT * FROM c WHERE c.customer_id = "CUST-5001"

-- 3. Get fraudulent transactions
SELECT c.transaction_id, c.customer_id, c.amount, c.merchant, c.timestamp
FROM c
WHERE c.is_fraud = true
ORDER BY c.amount DESC

-- 4. Get high-value transactions (over £500)
SELECT c.transaction_id, c.customer_id, c.amount, c.merchant, c.category
FROM c
WHERE c.amount > 500
ORDER BY c.amount DESC

-- 5. Count transactions by category
SELECT c.category, COUNT(1) as transaction_count
FROM c
GROUP BY c.category

-- 6. Total amount by merchant
SELECT c.merchant, SUM(c.amount) as total_amount, COUNT(1) as txn_count
FROM c
GROUP BY c.merchant
ORDER BY SUM(c.amount) DESC

-- 7. Transactions by payment method
SELECT c.payment_method, COUNT(1) as count, AVG(c.amount) as avg_amount
FROM c
GROUP BY c.payment_method

-- 8. Recent transactions (last 100)
SELECT TOP 100 *
FROM c
ORDER BY c.timestamp DESC

-- 9. Transactions in a specific location
SELECT * FROM c WHERE c.location = "London, UK"

-- 10. Average transaction amount by category
SELECT c.category,
       COUNT(1) as count,
       AVG(c.amount) as avg_amount,
       MIN(c.amount) as min_amount,
       MAX(c.amount) as max_amount
FROM c
GROUP BY c.category
ORDER BY AVG(c.amount) DESC
