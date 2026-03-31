"""
Synthetic data generator for e-commerce entities.

This module provides classes and methods to generate realistic synthetic data
for customers, orders, and products using the Faker library.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd
from faker import Faker

from .config import DATA_CONFIG, PRODUCT_CATEGORIES, ORDER_STATUSES

logger = logging.getLogger(__name__)

@dataclass
class Customer:
    """Customer data structure."""
    customer_id: int
    name: str
    email: str
    address: str
    signup_date: datetime

@dataclass
class Product:
    """Product data structure."""
    product_id: int
    name: str
    category: str
    price: float
    stock_quantity: int

@dataclass
class Order:
    """Order data structure."""
    order_id: int
    customer_id: int
    order_date: datetime
    total_amount: float
    status: str
    items: List[Dict[str, Any]]  # List of {"product_id": int, "quantity": int, "price": float}

class ECommerceDataGenerator:
    """
    Generator for synthetic e-commerce data.

    This class uses Faker to generate realistic synthetic data for customers,
    products, and orders with proper relationships between entities.
    """

    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the data generator.

        Args:
            seed: Random seed for reproducible data generation
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
        logger.info("Initialized ECommerceDataGenerator")

    def generate_customers(self, num_customers: int = DATA_CONFIG["num_customers"]) -> pd.DataFrame:
        """
        Generate synthetic customer data.

        Args:
            num_customers: Number of customers to generate

        Returns:
            DataFrame containing customer data
        """
        logger.info(f"Generating {num_customers} customers")
        customers = []

        for i in range(1, num_customers + 1):
            customer = Customer(
                customer_id=i,
                name=self.fake.name(),
                email=self.fake.email(),
                address=self.fake.address(),
                signup_date=self.fake.date_time_between(
                    start_date=DATA_CONFIG["start_date"],
                    end_date=DATA_CONFIG["end_date"]
                )
            )
            customers.append(customer)

        df = pd.DataFrame([vars(c) for c in customers])
        df['signup_date'] = pd.to_datetime(df['signup_date'])
        logger.info(f"Generated {len(df)} customers")
        return df

    def generate_products(self, num_products: int = DATA_CONFIG["num_products"]) -> pd.DataFrame:
        """
        Generate synthetic product data.

        Args:
            num_products: Number of products to generate

        Returns:
            DataFrame containing product data
        """
        logger.info(f"Generating {num_products} products")
        products = []

        for i in range(1, num_products + 1):
            category = self.fake.random_element(PRODUCT_CATEGORIES)
            product = Product(
                product_id=i,
                name=self._generate_product_name(category),
                category=category,
                price=round(self.fake.random.uniform(5.99, 999.99), 2),
                stock_quantity=self.fake.random_int(min=0, max=1000)
            )
            products.append(product)

        df = pd.DataFrame([vars(p) for p in products])
        logger.info(f"Generated {len(df)} products")
        return df

    def generate_orders(self, num_orders: int = DATA_CONFIG["num_orders"],
                       customers_df: Optional[pd.DataFrame] = None,
                       products_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Generate synthetic order data.

        Args:
            num_orders: Number of orders to generate
            customers_df: Customer data for reference
            products_df: Product data for reference

        Returns:
            DataFrame containing order data
        """
        logger.info(f"Generating {num_orders} orders")
        orders = []

        if customers_df is None or products_df is None:
            logger.warning("Customer or product data not provided, using default generation")

        for i in range(1, num_orders + 1):
            customer_id = self.fake.random_int(min=1, max=len(customers_df)) if customers_df is not None else i % 1000 + 1
            order_date = self.fake.date_time_between(
                start_date=DATA_CONFIG["start_date"],
                end_date=DATA_CONFIG["end_date"]
            )

            # Generate order items
            num_items = self.fake.random_int(min=1, max=5)
            items = []
            total_amount = 0.0

            for _ in range(num_items):
                if products_df is not None:
                    product = products_df.sample().iloc[0]
                    product_id = product['product_id']
                    price = product['price']
                else:
                    product_id = self.fake.random_int(min=1, max=100)
                    price = round(self.fake.random.uniform(5.99, 999.99), 2)

                quantity = self.fake.random_int(min=1, max=10)
                items.append({
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": price
                })
                total_amount += price * quantity

            order = Order(
                order_id=i,
                customer_id=customer_id,
                order_date=order_date,
                total_amount=round(total_amount, 2),
                status=self.fake.random_element(ORDER_STATUSES),
                items=items
            )
            orders.append(order)

        # Convert to DataFrame, exploding items
        orders_df = pd.DataFrame([{
            'order_id': o.order_id,
            'customer_id': o.customer_id,
            'order_date': o.order_date,
            'total_amount': o.total_amount,
            'status': o.status,
            'items': o.items
        } for o in orders])

        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
        logger.info(f"Generated {len(orders_df)} orders")
        return orders_df

    def _generate_product_name(self, category: str) -> str:
        """
        Generate a realistic product name based on category.

        Args:
            category: Product category

        Returns:
            Generated product name
        """
        category_templates = {
            "Electronics": ["Wireless {item}", "{brand} {item}", "Smart {item}"],
            "Clothing": ["{color} {item}", "{style} {item}", "Premium {item}"],
            "Home & Garden": ["{material} {item}", "Modern {item}", "{size} {item}"],
            "Sports & Outdoors": ["Professional {item}", "{brand} {item}", "Durable {item}"],
            "Books": ["{genre} Guide", "The Art of {topic}", "{topic} Handbook"],
            "Beauty & Personal Care": ["{brand} {item}", "Natural {item}", "Luxury {item}"],
            "Toys & Games": ["{theme} {item}", "Interactive {item}", "Educational {item}"],
            "Automotive": ["{brand} {item}", "Heavy Duty {item}", "Performance {item}"],
            "Health & Household": ["{brand} {item}", "Eco-Friendly {item}", "Advanced {item}"],
            "Grocery": ["Organic {item}", "Premium {item}", "Fresh {item}"]
        }

        templates = category_templates.get(category, ["{item}"])
        template = self.fake.random_element(templates)

        # Generate fake words for placeholders
        item_words = {
            "Electronics": ["Headphones", "Laptop", "Smartphone", "Tablet", "Speaker"],
            "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Shoes"],
            "Home & Garden": ["Chair", "Table", "Lamp", "Plant", "Decor"],
            "Sports & Outdoors": ["Ball", "Racket", "Bike", "Tent", "Gear"],
            "Books": ["Programming", "Cooking", "Travel", "History", "Science"],
            "Beauty & Personal Care": ["Cream", "Shampoo", "Perfume", "Brush", "Mask"],
            "Toys & Games": ["Puzzle", "Doll", "Board Game", "Building Set", "Vehicle"],
            "Automotive": ["Tire", "Oil", "Filter", "Battery", "Wiper"],
            "Health & Household": ["Cleaner", "Soap", "Towel", "Bottle", "Container"],
            "Grocery": ["Tea", "Coffee", "Pasta", "Rice", "Spice"]
        }

        items = item_words.get(category, ["Product"])
        brands = ["Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Amazon", "Generic"]
        colors = ["Red", "Blue", "Green", "Black", "White", "Gray"]
        styles = ["Casual", "Formal", "Sport", "Classic", "Modern"]
        materials = ["Wooden", "Metal", "Plastic", "Glass", "Fabric"]
        sizes = ["Small", "Medium", "Large", "Extra Large"]
        themes = ["Superhero", "Princess", "Space", "Animal", "Adventure"]
        genres = ["Mystery", "Romance", "Science Fiction", "Biography", "Self-Help"]

        return template.format(
            item=self.fake.random_element(items),
            brand=self.fake.random_element(brands),
            color=self.fake.random_element(colors),
            style=self.fake.random_element(styles),
            material=self.fake.random_element(materials),
            size=self.fake.random_element(sizes),
            theme=self.fake.random_element(themes),
            genre=self.fake.random_element(genres),
            topic=self.fake.random_element(["Python", "Data Science", "Machine Learning", "Web Development"])
        )

    def save_data(self, customers_df: pd.DataFrame, orders_df: pd.DataFrame,
                  products_df: pd.DataFrame, output_dir: str = "data/raw") -> None:
        """
        Save generated data to CSV files.

        Args:
            customers_df: Customer data
            orders_df: Order data
            products_df: Product data
            output_dir: Output directory path
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        customers_df.to_csv(f"{output_dir}/customers.csv", index=False)
        orders_df.to_csv(f"{output_dir}/orders.csv", index=False)
        products_df.to_csv(f"{output_dir}/products.csv", index=False)

        logger.info(f"Data saved to {output_dir}")