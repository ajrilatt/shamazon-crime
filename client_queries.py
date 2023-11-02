import psycopg2 as p
import random
import string
import time
import datetime


def gen_string(length=20):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

def gen_bool():
    return ''.join(random.choice(['FALSE', 'TRUE']))

def gen_int():
    return ''.join(str(random.choice(range(10))))

# Generates the report in response to truck_number crashing
def truck_crash_report(truck_number):
    truck_report_data = {'shipper_data': [], 'recipient_data': [], 'last_delivery': []}

    # Find all customers who had a package lost in the crash
    cursor.execute(f"""SELECT DISTINCT shipper_id, address FROM public.schmackage_logs JOIN public.schmackages ON schmackages.tracking_number=schmackage_logs.tracking_number
                   WHERE truck = {truck_number}""")
    for x in cursor.fetchall():
        truck_report_data['shipper_data'] = x[0]
        truck_report_data['recipient_data'] = x[1]

    # Find the last successful delivery by truck_number
    cursor.execute(f"""SELECT location FROM public.schmackage_logs
                   WHERE truck = {truck_number} ORDER BY timestamp DESC LIMIT 1""")
    truck_report_data['last_delivery'] = cursor.fetchall()
    return truck_report_data


# Finds the best customer in the past year
def most_frequent_customer():
    cursor.execute("""SELECT shipper_id FROM public.schmackage_logs JOIN public.schmackages
                   ON schmackage_logs.tracking_number = schmackages.tracking_number
                   WHERE timestamp BETWEEN LOCALTIMESTAMP - INTERVAL '1 YEAR' AND LOCALTIMESTAMP
                   GROUP BY shipper_id
                   ORDER BY COUNT(shipper_id) DESC LIMIT 1;""")
    return cursor.fetchone()


# Finds the customer that spent the most money in the past year
def most_spent_customer():
    cursor.execute("""SELECT shipper_id FROM public.schmackage_logs JOIN public.schmackages
                   ON schmackage_logs.tracking_number = schmackages.tracking_number
                   WHERE timestamp BETWEEN LOCALTIMESTAMP - INTERVAL '1 YEAR' AND LOCALTIMESTAMP
                   GROUP BY shipper_id ORDER BY SUM(price) LIMIT 1;""")
    return cursor.fetchone()


# Bills the customers for the past moneth of services
# 'simple_bill' => customer, address, and amount owed
# 'itemized_bill' => bill listing each individual shipment and the chargest for it
def bill_customers(bill_type='simple_bill'):
    # Creates simple bill
    if bill_type == 'simple_bill':
        cursor.execute("""SELECT shipper_id, return_address, SUM(price) FROM public.schmackages JOIN public.schmackage_logs
                       ON public.schmackages.tracking_number = public.schmackage_logs.tracking_number
                       WHERE timestamp BETWEEN LOCALTIMESTAMP - INTERVAL '1 MONTH' AND LOCALTIMESTAMP
                       GROUP BY shipper_id, return_address""")

    # Gives bill type by item
    elif bill_type == 'itemized_bill':
        cursor.execute("""SELECT shipper_id, schmackages.tracking_number, price FROM public.schmackages JOIN public.schmackage_logs
                       ON public.schmackages.tracking_number = public.schmackage_logs.tracking_number
                       WHERE timestamp BETWEEN LOCALTIMESTAMP - INTERVAL '1 MONTH' AND LOCALTIMESTAMP
                       GROUP BY shipper_id, schmackages.tracking_number""")

    elif bill_type == 'type':
        cursor.execute("""SELECT schmackages.shipper_id, return_address, SUM(price) FROM public.schmackages
                       JOIN public.schmackage_logs ON public.schmackages.tracking_number = public.schmackage_logs.tracking_number
                       JOIN public.schmucks ON schmucks.shipper_id = schmackages.shipper_id
                       WHERE timestamp BETWEEN LOCALTIMESTAMP - INTERVAL '1 MONTH' AND LOCALTIMESTAMP
                       GROUP BY crime_subscrimer, schmackages.shipper_id, return_address""")
    # Returns error message when invalid bill type is used
    else:
        return '''Error, invalid bill_type, please choose "simple_bill" or "itemized_bill"'''
    return cursor.fetchall()


def gen_schmucks(num=1000):
    for x in range(num):
        address = gen_string()
        email = gen_string()
        phone = gen_string(10)
        payment_info = gen_string()
        subscrimer = gen_bool()
        cursor.execute(f"""INSERT INTO public.schmucks(shipper_id, address, email, phone, payment_info, crime_subscrimer)
                       VALUES (gen_random_uuid(), '{address}', '{email}', '{phone}', '{payment_info}', {subscrimer});""")

# Generates random schmackages
def gen_schmackages_stuff(num=1000):
    for x in range(num):
        tracking_number = x
        address = gen_string()
        return_address = gen_string()
        package_type = random.choice(['flat envelope', 'small box', 'large box', 'crate', 'barrel', 'keg'])
        package_priority = random.choice(['dandelion tuft', 'phony express', 'standard', 'priority', 'first class', 'ethically and morally unsound speed'])
        shipping_considerations = gen_string()
        price = gen_int()
        cursor.execute(f"""INSERT INTO public.schmackages(shipper_id, tracking_number, address, package_weight, return_address, ptype, priority, shipping_considerations, price)
                       VALUES (gen_random_uuid(), {tracking_number}, '{address}', 1.2, '{return_address}', '{package_type}', '{package_priority}', '{shipping_considerations}', {price});""")

        gen_schmackage_logs(tracking_number)
            
            
#Generates random schmackage logs
def gen_schmackage_logs(tracking_number):
    for x in range(random.randint(0,10)):
        package_status = random.choice(['label printed at', 'departed from', 'arrived at', 'out for delivery', 'delivered'])
        location = gen_string()
        truck = x
        t = datetime.datetime.now()
        t2 = t + datetime.timedelta(seconds=x)
        ts = t2.strftime('%Y-%m-%d %H:%M:%S%z')
        cursor.execute(f"""INSERT INTO public.schmackage_logs(tracking_number, timestamp, package_status, location, truck)
                       VALUES ('{tracking_number}', '{ts}', '{package_status}', '{location}', {truck});""")

RESET_DB = [
    "DROP TABLE IF EXISTS public.schmucks;",
    "DROP TABLE IF EXISTS public.schmackage_logs;",
    "DROP TABLE IF EXISTS public.schmackages;"
]

#GENERATE_TEST_DATA = [
#    """INSERT INTO public.schmucks(shipper_id, address, email, phone, payment_info, crime_subscrimer) VALUES (
#        gen_random_uuid(), '101 N College Ave, Annville, PA 17003', 'yarnall@lvc.edu', '7171008995', '1234567890ABCDEFG', FALSE
#    );"""
#]

def reset_db(cursor):
    cursor.execute('''DROP TABLE IF EXISTS public.schmucks;''')
    cursor.execute('''DROP TABLE IF EXISTS public.schmackage_logs;
                      DROP TYPE package_status;
                      CREATE TYPE package_status AS ENUM ('label printed at', 'departed from', 'arrived at', 'out for delivery', 'delivered');''')
    cursor.execute('''DROP TABLE IF EXISTS public.schmackages;
                      DROP TYPE package_type;
                      DROP TYPE package_priority;
                      CREATE TYPE package_type AS ENUM ('flat envelope', 'small box', 'large box', 'crate', 'barrel', 'keg');
                      CREATE TYPE package_priority AS ENUM ('dandelion tuft', 'phony express', 'standard', 'priority', 'first class', 'ludicrous speed', 'ethically and morally unsound speed');''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS public.schmucks
    (
        shipper_id uuid NOT NULL,
        address character varying(128) COLLATE pg_catalog."default" NOT NULL,
        email character varying(128) COLLATE pg_catalog."default",
        phone character varying(10) COLLATE pg_catalog."default",
        payment_info character varying(128) COLLATE pg_catalog."default" NOT NULL,
        crime_subscrimer boolean NOT NULL,
        CONSTRAINT schmucks_pkey PRIMARY KEY (shipper_id)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.schmucks
        OWNER to postgres;''')



    cursor.execute('''
    CREATE TABLE IF NOT EXISTS public.schmackage_logs
    (
        tracking_number integer NOT NULL,
        "timestamp" timestamp with time zone NOT NULL,
        package_status character varying COLLATE pg_catalog."default",
        location character varying COLLATE pg_catalog."default",
        truck integer,
        CONSTRAINT schmackage_logs_pkey PRIMARY KEY (tracking_number, "timestamp")
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.schmackage_logs
        OWNER to postgres;''')


    cursor.execute('''
    CREATE TABLE IF NOT EXISTS public.schmackages
    (
        shipper_id uuid NOT NULL,
        tracking_number integer NOT NULL,
        address character varying(128) COLLATE pg_catalog."default" NOT NULL,
        return_address character varying(128) COLLATE pg_catalog."default" NOT NULL,
        package_weight double precision NOT NULL,
        ptype package_type NOT NULL,
        priority package_priority NOT NULL,
        shipping_considerations character varying(256) COLLATE pg_catalog."default",
        price double precision NOT NULL,
        CONSTRAINT schmackages_pkey PRIMARY KEY (tracking_number)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.schmackages
        OWNER to postgres;''')



if __name__ == "__main__":
    #Forms connection and sets up cursor to access SHAMAZON
    conn = p.connect(database = 'shamazon',
                     host = 'localhost',
                     user = 'postgres',
                     password = 'password',
                     port = '5432')
    cursor = conn.cursor()

    reset_db(cursor)
    gen_schmucks(1000)
    gen_schmackages_stuff(1000)
    cursor.execute("SELECT count(*) FROM schmucks")
    print(cursor.fetchall())
    cursor.execute("SELECT count(*) FROM schmackages")
    print(cursor.fetchall())
    cursor.execute("SELECT count(*) FROM schmackage_logs")
    print(cursor.fetchall())
    conn.commit()

print(truck_crash_report(7))
print(most_frequent_customer())
print(most_spent_customer())
print(bill_customers())
print(bill_customers('itemized_bill'))
print(bill_customers('type'))