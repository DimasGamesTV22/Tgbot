/*
  # Create repair service tables

  1. New Tables
    - `repair_services` - Stores available repair services and prices
      - `id` (uuid, primary key)
      - `name` (text) - Service name
      - `description` (text) - Service description
      - `price` (numeric) - Service price
      - `category` (text) - Service category
      - `created_at` (timestamp)

    - `repair_orders` - Stores customer repair orders
      - `id` (uuid, primary key)
      - `user_id` (bigint) - Telegram user ID
      - `service_id` (uuid) - Reference to repair_services
      - `status` (text) - Order status
      - `description` (text) - Problem description
      - `created_at` (timestamp)
      - `updated_at` (timestamp)

  2. Security
    - Enable RLS on both tables
    - Add policies for authenticated users
*/

-- Create repair services table
CREATE TABLE repair_services (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text,
  price numeric NOT NULL,
  category text NOT NULL,
  created_at timestamptz DEFAULT now()
);

-- Create repair orders table
CREATE TABLE repair_orders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id bigint NOT NULL,
  service_id uuid REFERENCES repair_services(id),
  status text NOT NULL DEFAULT 'pending',
  description text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Enable RLS
ALTER TABLE repair_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE repair_orders ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY "Allow read access to repair services"
  ON repair_services
  FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Allow read access to own repair orders"
  ON repair_orders
  FOR SELECT
  TO authenticated
  USING (auth.uid()::text = user_id::text);

-- Insert initial repair services
INSERT INTO repair_services (name, description, price, category) VALUES
  ('Диагностика ПК', 'Полная диагностика компьютера', 1500, 'Диагностика'),
  ('Чистка от пыли', 'Профессиональная чистка компьютера от пыли', 2000, 'Обслуживание'),
  ('Замена термопасты', 'Замена термопасты на процессоре', 1000, 'Обслуживание'),
  ('Установка Windows', 'Установка операционной системы Windows', 2500, 'Программное обеспечение'),
  ('Замена HDD/SSD', 'Замена жесткого диска или SSD', 1500, 'Комплектующие'),
  ('Сборка ПК', 'Профессиональная сборка компьютера', 5000, 'Сборка');