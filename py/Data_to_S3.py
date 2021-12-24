import boto3

s3 = boto3.resource('s3')
bucket = 'acadataengineeringproject'

filename = 'data_guest.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)


filename = 'tour_agent.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Countries.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Final_restaurant.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = "Final_Tours.csv"
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Final_Hotel.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Final_Room.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Final_Room_type.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'hotel-reviews.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Payment_type.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Payment_status.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Payment.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Service_booking.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Review.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'Booking.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)

filename = 'booking_status.csv'
s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=filename)