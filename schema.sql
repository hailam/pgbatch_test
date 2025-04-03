-- Migration script for testing PostgreSQL Batch Service
-- Creates test tables with sample data

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert sample data
-- Users
INSERT INTO users (name, email) VALUES
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com'),
('Bob Johnson', 'bob@example.com'),
('Alice Brown', 'alice@example.com'),
('Charlie Davis', 'charlie@example.com');

-- Products
INSERT INTO products (name, price, stock_quantity) VALUES
('Laptop', 1299.99, 10),
('Smartphone', 699.99, 20),
('Headphones', 149.99, 30),
('Monitor', 299.99, 15),
('Keyboard', 89.99, 25),
('Mouse', 49.99, 40),
('Tablet', 399.99, 12),
('Printer', 199.99, 8),
('External Hard Drive', 129.99, 18),
('Webcam', 79.99, 22);

-- Orders
INSERT INTO orders (user_id, total_amount, status) VALUES
(1, 1349.98, 'completed'),
(2, 849.98, 'processing'),
(3, 199.99, 'completed'),
(4, 1099.97, 'pending'),
(1, 89.99, 'processing');

-- Order Items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),
(1, 6, 1, 49.99),
(2, 2, 1, 699.99),
(2, 3, 1, 149.99),
(3, 8, 1, 199.99),
(4, 5, 1, 89.99),
(4, 7, 1, 399.99),
(4, 10, 1, 79.99),
(5, 5, 1, 89.99);

-- Create indexes for better performance
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);