import csv
from matplotlib import pyplot
from collections import Counter
import pandas as pd

def read_csv():
    totalPriceSale = []
    with open('out/totalPriceSale.csv/part-00000-f7999483-29ef-46e0-822c-c07286287efd-c000.csv') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            year = row["year"]
            total_price = row["total_price"]
            totalPriceSale.append({'year':year,'total_price':total_price})
    pyplot.barh(totalPriceSale['year'],totalPriceSale['total_price'],
            color='blue',label="year / total_price")
    pyplot.show()

if __name__ == "__main__":
    read_csv()
    

