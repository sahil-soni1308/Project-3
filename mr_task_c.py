from mrjob.job import MRJob
import csv

class PaymentTypeCount(MRJob):
	def mapper(self, _, line):
	
		# skipping header line
		if line.startswith('ID'):
			return
			
		# parsing the CSV line
		fields = list(csv.reader([line]))[0]
		
		# extracting the payment type and emit a count of 1
		payment_type = fields[10]
		yield payment_type, 1

	def combiner(self, payment_type, counts):
		# sum up the counts for each payment type
		yield payment_type, sum(counts)

	def reducer(self, payment_type, counts):
		# sum up the counts for each payment type
		yield payment_type, sum(counts)

if __name__ == '__main__':
PaymentTypeCount.run()