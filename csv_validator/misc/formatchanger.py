import csv
from datetime import datetime

input_file = 'cdfp_test_fk_valid.csv'
output_file = 'cdfp_test_fk_valid_new_format.csv'

with open(input_file, mode='r', newline='') as infile, open(output_file, mode='w', newline='') as outfile:
    reader = csv.reader(infile, delimiter='|')
    writer = csv.writer(outfile, delimiter='|')

    for row in reader:
        if row[3] == 'Date':
            writer.writerow(row)
        # Change the date format in the 4th column (index 3)
        else:
            date_str = row[3]
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            new_date_str = date_obj.strftime('%Y/%b/%d')
            row[3] = new_date_str
            writer.writerow(row)

print("Date format updated successfully.")