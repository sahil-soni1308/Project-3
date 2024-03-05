from mrjob.job import MRJob
import csv
from mrjob.step import MRStep

class TipsToRevenueRatio(MRJob):
	def mapper(self, _, line):
		# skip header row
		if line.startswith('ID'):
			return

		# Parse the CSV line
		row = next(csv.reader([line]))
		
		# Extract the relevant columns
		pickup_location = int(row[8])
		tip_amount = float(row[14])
		total_amount = float(row[17])
		
		# Emit the pickup location and the tip to revenue ratio
		if total_amount > 0:
			yield pickup_location, tip_amount / total_amount
			
	def reducer(self, key, values):
		# calculate the average tip to revenue ratio for each pickup location
		tip_to_revenue_ratios = list(values)
		avg_tip_to_revenue_ratio = round(sum(tip_to_revenue_ratios) / len(tip_to_revenue_ratios), 2)
		yield key, avg_tip_to_revenue_ratio
		
# defines the two steps of the MapReduce job in the First steps and reducer_sort in the second steps
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				reducer=self.reducer),
			MRStep(mapper=None,
				reducer=self.reducer_sort)
		]
		
# sorts the output by pickup location and average ratio.
	def reducer_sort(self, key, values):
		for value in sorted(values):
			yield key, value

if __name__ == '__main__':
	TipsToRevenueRatio.run()