from mrjob.job import MRJob
from datetime import datetime

class MRRevenueByTime(MRJob):

	def mapper(self, _, line):
		fields = line.split(',')
		if fields[0] == 'ID':
			return
		pickup_time = datetime.strptime(fields[2], '%Y-%m-%d %H:%M:%S')
		revenue = float(fields[17])
		
## extracts the year, month, hour, and weekday

	yield (pickup_time.year, pickup_time.month, pickup_time.hour, pickup_time.weekday()), (revenue, 1)

	def reducer(self, key, values):
		total_revenue = 0
		total_trips = 0
		
		##calculates the total revenue and total number of trips for each group
		for revenue, trips in values:
			total_revenue += revenue
			total_trips += trips
		yield key, round(total_revenue / total_trips,2 )

if __name__ == '__main__':
	MRRevenueByTime.run()