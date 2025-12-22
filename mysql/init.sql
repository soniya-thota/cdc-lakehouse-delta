CREATE TABLE IF NOT EXISTS inventory.orders (
id INT PRIMARY KEY,
customer VARCHAR(64),
amount DECIMAL(10,2),
status VARCHAR(16),
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


INSERT INTO inventory.orders (id, customer, amount, status) VALUES
(1, 'Alice', 120.50, 'new'),
(2, 'Bob', 75.00, 'new'),
(3, 'Cara', 33.25, 'new');
