from functools import reduce
import sys
import csv

def map_reduce(input_file):
# reading CSV file
	with open(input_file) as f:
		reader = csv.reader(f)
		
		# skip the header row
		next(reader)
		
		# map the pickup location ID to the total amount charged
		mapped_values = map(lambda row: (row[8], float(row[17])), reader)

		# Group the mapped values by pickup location ID
		grouped_values = {}
		for pickup_loc, total_amount in mapped_values:
			if pickup_loc in grouped_values:
				grouped_values[pickup_loc].append(total_amount)
			else:
				grouped_values[pickup_loc] = [total_amount]
		# reduce the grouped values to find the pickup location that generated the most revenue
		reduced_values = reduce(lambda x, y: (x[0], x[1]) if x[1] > y[1] else (y[0], y[1]), grouped_values.items())
		# print the result
		print(f"Pickup location {reduced_values[0]} generated the most revenue: {reduced_values[1]}")
if __name__ == '__main__':
	# get the input file name from the command line arguments
	input_file = sys.argv[1]
	
	# Call the map_reduce function with the input file name
	map_reduce(input_file)