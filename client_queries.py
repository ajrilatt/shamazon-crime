import psycopg2 as p
import random
import string

#Forms connection and sets up cursor to access SHAMAZON
conn = p.connect(database = 'shamazon',
                 host = 'localhost',
                 user = input('Username: '),
                 password = input('Password: '),
                 port = '5432')
cursor = conn.cursor()

#Returns all packages
cursor.execute("SELECT * FROM schmackages")
print(cursor.fetchone())

def gen_string(length = 20):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

def gen_bool():
    return ''.join(random.choice(['FALSE', 'TRUE']) for i in range(length))
    
#Generates the report in response to truck_number crashing
def truck_crash_report(truck_number):
    truck_report_data = {'shipper_data': [], 'recipient_data': [], 'last_delivery': []}
    
    #Find all customers who had a package lost in the crash
    cursor.execute(f"""SELECT UNIQUE shipper_id FROM schmackage_logs
                   WHERE truck = {truck_number}""")
    truck_report_data['shipper_data'] = cursor.fetchall()
    
    #Find the recipients who had a package lost in the crash
    cursor.execute(f"""SELECT UNIQUE address FROM schmackage_logs
                   WHERE truck = {truck_number}""") 
    truck_report_data['recipient_data'] = cursor.fetchall()
    
    #Find the last successful delivery by truck_number
    cursor.execute(f"""SELECT location FROM schmackage_logs
                   WHERE truck = {truck_number} GROUP BY max(timestamp) LIMIT 1""")
    truck_report_data['last_delivery'] = cursor.fetchall()
    return truck_report_data

#Finds the best customer in the past year
def most_frequent_customer():
    cursor.execute("""SELECT shipper_id FROM schmackage_logs, schmackages
                   WHERE schmackage_logs.tracking_number = schmackages.tracking_number AND
                   DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -12, getdata()))
                   GROUP BY shipper_id ORDER BY price LIMIT 1;""")
    return cursor.fetchone()

#Finds the customer that spent the most money in the past year
def most_spent_customer():
    cursor.execute("""SELECT shipper_id FROM schmackages
                   WHERE DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -12, getdata()))
                   GROUP BY shipper_id ORDER BY SUM(price) LIMIT 1;""")
    return cursor.fetchone()

#Bills the customers for the past moneth of services
#'simple_bill' => customer, address, and amount owed
#'itemized_bill' => bill listing each individual shipment and the chargest for it
def bill_customers(bill_type = 'simple_bill'):

    #Creates simple bill
    if bill_type == 'simple_bill':
        cursor.execute("""SELECT shipper_id, return_address, SUM(price) FROM schmackages
                       WHERE DATEPART(m, timestamp) = DATEPART(m, DATEADD(m, -1, getdate())) AND
                       DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -1, getdate()))
                       GROUP BY shipper_id""")

    #Gives bill type by item
    elif bill_type == 'itemized_bill':
        cursor.execute("""SELECT shipper_id, tracking_number, price FROM schmackages, schmackage_logs
                       WHERE schmackages.tracking_number = schmackage_logs.tracking_number AND
                       DATEPART(m, timestamp) = DATEPART(m, DATEADD(m, -1, getdate())) AND
                       DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -1, getdate()))
                       GROUP BY shipper_id""")

    #Returns error message when invalid bill type is used
    else:
        return '''Error, invalid bill_type, please choose "simple_bill" or "itemized_bill"'''
    return cursor.fetchall()

#Generates random schmucks
def gen_schmucks(num):
    for x in range(num):
        address = gen_string()
        email = gen_string()
        phone = gen_string()
        payment_info = gen_string()
        subscrimer = gen_bool()
        cursor.execute("""INSERT INTO public.schmucks(shipper_id, address, email, phone, payment_info, crime_subscrimer)
                       VALUES (gen_random_uuid(), '{address}', '{email}', '{phone}', '{payment_info}', {subscrimer})""")
