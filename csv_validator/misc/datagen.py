import csv
import uuid
from faker import Faker
from datetime import date
import random


fake = Faker()

header = ["ID", "Email", "Sales_Amount", "Date", "Region", "Drug_Type", "Units_Sold"]
regions = ["South America", "Asia", "North America", "Africa", "Europe", "Oceania"]
drug_types = ["Antibiotic", "Vaccine", "Analgesic", "Cardiovascular", "Antiviral"]

with open('cdfp_test_fk_valid.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter='|')
    
    for _ in range(100000):
        row = [
            str(uuid.uuid4()),
            fake.email(),
            round(random.uniform(1000, 100000), 2),
            fake.date_between(start_date=date(2023,1,1), end_date=date(2025,2,1)).strftime('%Y-%m-%d'),
            random.choice(regions),
            random.choice(drug_types),
            round(random.uniform(50, 10000), 2)
        ]
        writer.writerow(row)