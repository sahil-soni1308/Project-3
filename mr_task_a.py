from mrjob.job import MRJob
import csv

class TripRevenue(MRJob):
# mapper function reads each line of input file, extracts vendor ID and total amount for each trip.
	def mapper(self, _, line):
		row = list(csv.reader([line]))[0]
		try:
			vendor_id = int(row[1])
			total_amount = float(row[17])
			yield vendor_id, total_amount
		except:
			pass

# reducer function receives key-value pairs from mapper and calculates total revenue generated by each vendor by summing up total amounts for all their trips.
	def reducer(self, key, values):
		total_revenue = sum(values)
		yield key, total_revenue

# combiner function is used to aggregate the intermediate outputs from the mapper
	def combiner(self, key, values):
		total_revenue = sum(values)
		yield key, total_revenue
if __name__ == '__main__':
	TripRevenue.run()