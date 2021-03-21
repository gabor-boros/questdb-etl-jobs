"""
Generate random purchase data in a CSV format.
"""

import csv
import string
from pathlib import Path
from datetime import datetime
from random import choice, randint

PURCHASE_COUNT = randint(100, 500)


def random_chars(length: int) -> str:
    """
    Return random sequence of N characters.
    """

    return "".join(choice(string.ascii_lowercase) for _ in range(length))


def random_datetime() -> datetime:
    """
    Return a datetime object with randomized minutes and seconds for the current
    hour.
    """

    now = datetime.now()
    return datetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=randint(0, 59),
        second=randint(0, 59),
    )


def main():
    """
    Do random data generation.
    """

    target_file = Path(f"./{random_chars(12)}.csv")

    # Generate email address
    emails = [
        f"{random_chars(choice([6,7,8,9,10,11,12]))}@example.com"
        for _ in range(PURCHASE_COUNT)
    ]

    # Generate items and their price
    items = [(randint(100, 500), randint(1, 200)) for _ in range(PURCHASE_COUNT)]
    data = []

    for email in emails:
        item_id, item_price = choice(items)
        data.append(
            [
                email,
                item_id,
                randint(1, 10),
                item_price,
                random_datetime().isoformat(),
            ]
        )

    # Sort the data by purchase date to avoid out of order exceptions raised
    # by QuestDB as of 2021-03-21
    data.sort(key=lambda r: r[-1])

    # Write a CSV file of random data with the following column order:
    # email address, purchased item's ID, quantity, price per item, purchase date
    with open(target_file.absolute(), "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=",")
        writer.writerows(data)

    print(target_file.absolute(), "generated")


if __name__ == "__main__":
    main()
