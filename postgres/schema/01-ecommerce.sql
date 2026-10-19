
CREATE TABLE IF NOT EXISTS public.users (
    user_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);

CREATE TABLE IF NOT EXISTS public.ecommerce_orders (
    order_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES public.users(user_id),
    order_date TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'paid', 'shipped'))
);

CREATE TABLE IF NOT EXISTS public.ecommerce_order_items (
    order_id UUID NOT NULL
        REFERENCES public.ecommerce_orders(order_id),
    product_id TEXT NOT NULL
        REFERENCES public.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL
        CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, product_id)
);
