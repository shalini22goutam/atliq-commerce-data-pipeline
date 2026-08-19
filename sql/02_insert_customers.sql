-- ============================================================
--  AtliQ Commerce OLTP  |  Seed data: customers
--  Rows: 40
-- ============================================================
SET NOCOUNT ON;
SET IDENTITY_INSERT dbo.customers ON;

INSERT INTO dbo.customers (customer_id, customer_name, email, city, signup_date, updated_at) VALUES
    (1, N'Rahul Verma', N'rahul.verma1@example.com', N'Chennai', '2025-11-20', '2025-11-20 11:00:00'),
    (2, N'Krishna Patel', N'krishna.patel2@example.com', N'Jaipur', '2025-10-11', '2025-10-11 09:00:00'),
    (3, N'Meera Gupta', N'meera.gupta3@example.com', N'Bengaluru', '2025-11-16', '2025-11-16 09:00:00'),
    (4, N'Reyansh Reddy', N'reyansh.reddy4@example.com', N'Surat', '2025-03-19', '2025-03-19 08:00:00'),
    (5, N'Kavya Reddy', N'kavya.reddy5@example.com', N'Jaipur', '2024-12-01', '2024-12-01 14:00:00'),
    (6, N'Krishna Singh', N'krishna.singh6@example.com', N'Chennai', '2025-02-04', '2025-02-04 20:00:00'),
    (7, N'Nikhil Sharma', N'nikhil.sharma7@example.com', N'Kolkata', '2025-09-12', '2025-09-12 13:00:00'),
    (8, N'Ishaan Patel', N'ishaan.patel8@example.com', N'Pune', '2025-08-14', '2025-08-14 09:00:00'),
    (9, N'Aditya Gupta', N'aditya.gupta9@example.com', N'Pune', '2025-10-14', '2025-10-14 13:00:00'),
    (10, N'Neha Nair', N'neha.nair10@example.com', N'Ahmedabad', '2025-11-10', '2025-11-10 16:00:00'),
    (11, N'Vihaan Malhotra', N'vihaan.malhotra11@example.com', N'Mumbai', '2025-05-23', '2025-05-23 16:00:00'),
    (12, N'Rohan Bose', N'rohan.bose12@example.com', N'Surat', '2025-01-15', '2025-01-15 13:00:00'),
    (13, N'Meera Reddy', N'meera.reddy13@example.com', N'Mumbai', '2024-12-07', '2024-12-07 08:00:00'),
    (14, N'Karan Reddy', N'karan.reddy14@example.com', N'Mumbai', '2025-07-07', '2025-07-07 11:00:00'),
    (15, N'Nikhil Verma', N'nikhil.verma15@example.com', N'Chennai', '2025-05-22', '2025-05-22 15:00:00'),
    (16, N'Rahul Bose', N'rahul.bose16@example.com', N'Delhi', '2025-05-30', '2025-05-30 13:00:00'),
    (17, N'Diya Reddy', N'diya.reddy17@example.com', N'Chennai', '2024-12-24', '2024-12-24 19:00:00'),
    (18, N'Anjali Rao', N'anjali.rao18@example.com', N'Mumbai', '2025-01-05', '2025-01-05 17:00:00'),
    (19, N'Rahul Patel', N'rahul.patel19@example.com', N'Hyderabad', '2025-03-04', '2025-03-04 10:00:00'),
    (20, N'Priya Gupta', N'priya.gupta20@example.com', N'Jaipur', '2025-07-17', '2025-07-17 11:00:00'),
    (21, N'Karan Iyer', N'karan.iyer21@example.com', N'Hyderabad', '2025-11-04', '2025-11-04 08:00:00'),
    (22, N'Sneha Iyer', N'sneha.iyer22@example.com', N'Chennai', '2025-05-11', '2025-05-11 09:00:00'),
    (23, N'Reyansh Malhotra', N'reyansh.malhotra23@example.com', N'Pune', '2025-02-15', '2025-02-15 11:00:00'),
    (24, N'Rahul Singh', N'rahul.singh24@example.com', N'Ahmedabad', '2025-05-14', '2025-05-14 10:00:00'),
    (25, N'Krishna Kulkarni', N'krishna.kulkarni25@example.com', N'Jaipur', '2025-02-18', '2025-02-18 12:00:00'),
    (26, N'Ravi Joshi', N'ravi.joshi26@example.com', N'Surat', '2025-04-27', '2025-04-27 14:00:00'),
    (27, N'Arjun Mehta', N'arjun.mehta27@example.com', N'Mumbai', '2025-03-25', '2025-03-25 20:00:00'),
    (28, N'Vivaan Bose', N'vivaan.bose28@example.com', N'Delhi', '2025-10-07', '2025-10-07 18:00:00'),
    (29, N'Sai Das', N'sai.das29@example.com', N'Kolkata', '2024-12-19', '2024-12-19 17:00:00'),
    (30, N'Aadhya Joshi', N'aadhya.joshi30@example.com', N'Jaipur', '2025-04-07', '2025-04-07 12:00:00'),
    (31, N'Kavya Bose', N'kavya.bose31@example.com', N'Mumbai', '2025-11-27', '2025-11-27 18:00:00'),
    (32, N'Deepak Mehta', N'deepak.mehta32@example.com', N'Pune', '2025-07-19', '2025-07-19 09:00:00'),
    (33, N'Rohan Gupta', N'rohan.gupta33@example.com', N'Ahmedabad', '2025-09-13', '2025-09-13 08:00:00'),
    (34, N'Ravi Malhotra', N'ravi.malhotra34@example.com', N'Chennai', '2024-11-29', '2024-11-29 16:00:00'),
    (35, N'Amit Patel', N'amit.patel35@example.com', N'Mumbai', '2025-03-18', '2025-03-18 18:00:00'),
    (36, N'Rahul Mehta', N'rahul.mehta36@example.com', N'Hyderabad', '2025-01-25', '2025-01-25 10:00:00'),
    (37, N'Diya Das', N'diya.das37@example.com', N'Jaipur', '2025-09-11', '2025-09-11 20:00:00'),
    (38, N'Anjali Mehta', N'anjali.mehta38@example.com', N'Surat', '2025-12-02', '2025-12-02 13:00:00'),
    (39, N'Isha Sharma', N'isha.sharma39@example.com', N'Pune', '2025-10-06', '2025-10-06 20:00:00'),
    (40, N'Rohan Reddy', N'rohan.reddy40@example.com', N'Hyderabad', '2025-11-03', '2025-11-03 17:00:00');

SET IDENTITY_INSERT dbo.customers OFF;
GO
