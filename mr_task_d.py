from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from itertools import groupby
import csv

class TripTime(MRJob):

	def mapper(self, _, line):
		if line.startswith('ID'):
			return
# parsing the CSV line
		data = next(csv.reader([line]))
# each line of input data is parsed and the pickup location, dropoff location, pickup time, and dropoff time are extracted
		try:
			pickup_loc = int(data[8])
			dropoff_loc = int(data[9])
			pickup_time = datetime.strptime(data[2], '%Y-%m-%d %H:%M:%S')
			dropoff_time = datetime.strptime(data[3], '%Y-%m-%d %H:%M:%S')
			yield pickup_loc, (dropoff_time - pickup_time).seconds / 60
		except:
			pass
			
# trip time is calculated by subtracting the pickup time from the dropoff time, converting the result to minutes
	def reducer(self, pickup_loc, trip_times):
		total_trip_time = 0
		count = 0
# trip times for each pickup location are aggregated by calculating the total trip time and the count of trips for that pickup location
		for trip_time in trip_times:
			total_trip_time += trip_time
			count += 1
		yield pickup_loc, round(total_trip_time / count, 2)

# return a list of steps that define the job
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				reducer=self.reducer)
		]

if __name__ == '__main__':
	TripTime.run()