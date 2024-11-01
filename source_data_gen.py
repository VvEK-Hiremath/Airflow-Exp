import json, calendar
from datetime import datetime, date
from faker import Faker
from random import randrange, choice

CUSTOMERS = 20
ORDERS = 50
REVIEWS = 30

REVIEW_LINES = [
    [
        "This is an excellent product, I would happily recommend it.",
        "The product works as expected, no major issues.",
        "I'm unsatisfied, this product broke after only using it for two days.",
    ],
    [
        "The company's sales and support was very good.",
        "I didn't need to contact the company after placing my order.",
        "I sent the company a message and it took a long time to get a response.",
    ],
    [
        "I'm satisfied and would order again.",
        "The cost was reasonable, I may order again.",
        "I would not order this product again.",
    ],
]

YEAR = date.today().year
MONTH = date.today().month
LAST_DAY = calendar.monthrange(YEAR, MONTH)[1]

fake = Faker()


def lambda_handler(event, context):

    path = event.get("rawPath", None)

    if path.endswith("/customers"):
        return json.dumps(create_customers(CUSTOMERS, YEAR, MONTH, LAST_DAY))

    if path.endswith("/orders"):
        return json.dumps(create_orders(ORDERS, CUSTOMERS, YEAR, MONTH, LAST_DAY))

    if path.endswith("/reviews"):
        return json.dumps(create_reviews(REVIEWS, ORDERS, YEAR, MONTH, LAST_DAY))

    return json.dumps({})


def create_customers(number, year, month, last_day):
    customers = []
    for i in range(number):
        create_date = fake.date_time_between_dates(
            datetime_start=datetime(year, month, 1),
            datetime_end=datetime(year, month, last_day),
        )
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name}.{last_name}@{fake.domain_name()}".lower()
        customers.append(
            {
                "customer_id": i,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "created": str(create_date),
            }
        )

    return customers


def create_orders(number, customer_range, year, month, last_day):
    orders = []

    for i in range(number):
        amount = fake.pydecimal(left_digits=3, right_digits=2, positive=True)
        order_datetime = fake.date_time_between_dates(
            datetime_start=datetime(year, month, 1),
            datetime_end=datetime(year, month, last_day),
        )
        orders.append(
            {
                "order_id": i,
                "customer_id": randrange(1, customer_range),
                "amount": str(amount),
                "datetime": str(order_datetime),
            }
        )

    return orders


def create_reviews(number, order_range, year, month, last_day):
    reviews = []
    for i in range(number):
        review = "/n/n".join(
            [
                choice(REVIEW_LINES[0]),
                choice(REVIEW_LINES[1]),
                choice(REVIEW_LINES[2]),
            ]
        )
        reviews.append(
            {
                "review_id": i,
                "order_id": randrange(1, order_range),
                "review": review,
            }
        )

    return reviews
