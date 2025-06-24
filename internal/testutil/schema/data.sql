-- Create some test data
INSERT INTO public.users (username, email, password_hash, first_name, last_name, profile_data, tags, roles) VALUES
('john_doe', 'john@example.com', 'hash123', 'John', 'Doe', '{"age": 30, "city": "New York", "interests": ["coding", "music"]}', ARRAY['developer', 'music'], ARRAY['user', 'admin']),
('jane_smith', 'jane@example.com', 'hash456', 'Jane', 'Smith', '{"age": 25, "city": "San Francisco", "interests": ["art", "travel"]}', ARRAY['designer', 'travel'], ARRAY['user']),
('bob_wilson', 'bob@example.com', 'hash789', 'Bob', 'Wilson', '{"age": 35, "city": "Chicago", "interests": ["sports", "cooking"]}', ARRAY['sports', 'cooking'], ARRAY['user', 'moderator']);

INSERT INTO public.products (name, description, price, cost, weight_kg, dimensions_cm, category, tags, attributes) VALUES
('Laptop Pro', 'High-performance laptop', 1299.99, 800.00, 2.5, ARRAY[35.5, 24.3, 1.8], 'Electronics', ARRAY['laptop', 'computer', 'tech'], '{"brand": "TechCorp", "cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD"}'),
('Wireless Mouse', 'Ergonomic wireless mouse', 49.99, 15.00, 0.12, ARRAY[12.5, 6.8, 3.2], 'Electronics', ARRAY['mouse', 'wireless', 'ergonomic'], '{"brand": "TechCorp", "battery_life": "6 months", "dpi": "1200"}'),
('Coffee Mug', 'Ceramic coffee mug', 12.99, 3.00, 0.35, ARRAY[8.5, 8.5, 12.0], 'Kitchen', ARRAY['mug', 'ceramic', 'coffee'], '{"brand": "HomeGoods", "capacity": "350ml", "microwave_safe": true}');

INSERT INTO public.orders (user_id, order_number, status, total_amount, tax_amount, shipping_address, billing_address, items) VALUES
(1, 'ORD-001', 'completed', 1349.98, 108.00, '{"street": "123 Main St", "city": "New York", "zip": "10001"}', '{"street": "123 Main St", "city": "New York", "zip": "10001"}', '[{"product_id": 1, "quantity": 1, "price": 1299.99}, {"product_id": 2, "quantity": 1, "price": 49.99}]'),
(2, 'ORD-002', 'pending', 12.99, 1.04, '{"street": "456 Oak Ave", "city": "San Francisco", "zip": "94102"}', '{"street": "456 Oak Ave", "city": "San Francisco", "zip": "94102"}', '[{"product_id": 3, "quantity": 1, "price": 12.99}]');

INSERT INTO analytics.page_views (user_id, page_url, page_title, referrer, user_agent, ip_address, session_id, view_duration_seconds, interactions) VALUES
(1, '/products/laptop-pro', 'Laptop Pro - TechCorp', 'https://google.com', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '192.168.1.100', 'sess_123', 45, '{"clicks": 3, "scrolls": 5, "time_on_page": 45}'),
(2, '/products/wireless-mouse', 'Wireless Mouse - TechCorp', 'https://bing.com', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '192.168.1.101', 'sess_124', 30, '{"clicks": 2, "scrolls": 3, "time_on_page": 30}'),
(NULL, '/home', 'Home - TechCorp', NULL, 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1)', '192.168.1.102', 'sess_125', 15, '{"clicks": 1, "scrolls": 2, "time_on_page": 15}');

INSERT INTO public.complex_data (name, string_array, int_array, float_array, bool_array, simple_json, nested_json, array_json) VALUES
('Test Data 1', 
 ARRAY['apple', 'banana', 'cherry'], 
 ARRAY[1, 2, 3, 4, 5], 
 ARRAY[1.1, 2.2, 3.3], 
 ARRAY[true, false, true], 
 '{"key": "value", "number": 42}', 
 '{"user": {"name": "John", "age": 30}, "settings": {"theme": "dark"}}', 
 '[{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]'),
('Test Data 2', 
 ARRAY['red', 'green', 'blue'], 
 ARRAY[10, 20, 30], 
 ARRAY[0.1, 0.2, 0.3], 
 ARRAY[false, true, false], 
 '{"status": "active", "score": 95.5}', 
 '{"config": {"timeout": 30, "retries": 3}, "features": {"cache": true}}', 
 '[{"type": "A", "value": 100}, {"type": "B", "value": 200}]');