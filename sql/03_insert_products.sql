-- ============================================================
--  AtliQ Commerce OLTP  |  Seed data: products
--  Rows: 25
-- ============================================================
SET NOCOUNT ON;
SET IDENTITY_INSERT dbo.products ON;

INSERT INTO dbo.products (product_id, product_name, category, unit_price, updated_at) VALUES
    (1, N'Wireless Earbuds', N'Electronics', 2499, '2025-12-15 10:00:00'),
    (2, N'Bluetooth Speaker', N'Electronics', 3299, '2025-12-15 10:00:00'),
    (3, N'Power Bank 20000mAh', N'Electronics', 1799, '2025-12-15 10:00:00'),
    (4, N'USB-C Charger 65W', N'Electronics', 1499, '2025-12-15 10:00:00'),
    (5, N'Smartwatch', N'Electronics', 4999, '2025-12-15 10:00:00'),
    (6, N'Non-stick Frying Pan', N'Home & Kitchen', 899, '2025-12-15 10:00:00'),
    (7, N'Electric Kettle', N'Home & Kitchen', 1299, '2025-12-15 10:00:00'),
    (8, N'Steel Water Bottle', N'Home & Kitchen', 549, '2025-12-15 10:00:00'),
    (9, N'Storage Container Set', N'Home & Kitchen', 749, '2025-12-15 10:00:00'),
    (10, N'LED Desk Lamp', N'Home & Kitchen', 999, '2025-12-15 10:00:00'),
    (11, N'Cotton T-Shirt', N'Fashion', 599, '2025-12-15 10:00:00'),
    (12, N'Running Shoes', N'Fashion', 2199, '2025-12-15 10:00:00'),
    (13, N'Leather Wallet', N'Fashion', 1099, '2025-12-15 10:00:00'),
    (14, N'Backpack', N'Fashion', 1599, '2025-12-15 10:00:00'),
    (15, N'Sunglasses', N'Fashion', 899, '2025-12-15 10:00:00'),
    (16, N'Face Wash', N'Beauty', 299, '2025-12-15 10:00:00'),
    (17, N'Moisturizer', N'Beauty', 449, '2025-12-15 10:00:00'),
    (18, N'Sunscreen SPF50', N'Beauty', 549, '2025-12-15 10:00:00'),
    (19, N'Yoga Mat', N'Sports', 799, '2025-12-15 10:00:00'),
    (20, N'Dumbbell Set 10kg', N'Sports', 1899, '2025-12-15 10:00:00'),
    (21, N'Skipping Rope', N'Sports', 249, '2025-12-15 10:00:00'),
    (22, N'Data Engineering Guide', N'Books', 699, '2025-12-15 10:00:00'),
    (23, N'Python Crash Course', N'Books', 799, '2025-12-15 10:00:00'),
    (24, N'SQL for Analysts', N'Books', 599, '2025-12-15 10:00:00'),
    (25, N'The Lean Startup', N'Books', 499, '2025-12-15 10:00:00');

SET IDENTITY_INSERT dbo.products OFF;
GO
