import psycopg2 as p
import random
import string


def gen_string(length=20):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


def gen_bool():
    return ''.join(random.choice(['FALSE', 'TRUE']))


# Generates the report in response to truck_number crashing
def truck_crash_report(truck_number):
    truck_report_data = {'shipper_data': [], 'recipient_data': [], 'last_delivery': []}

    # Find all customers who had a package lost in the crash
    cursor.execute(f"""SELECT UNIQUE shipper_id FROM schmackage_logs
                   WHERE truck = {truck_number}""")
    truck_report_data['shipper_data'] = cursor.fetchall()

    # Find the recipients who had a package lost in the crash
    cursor.execute(f"""SELECT UNIQUE address FROM schmackage_logs
                   WHERE truck = {truck_number}""")
    truck_report_data['recipient_data'] = cursor.fetchall()

    # Find the last successful delivery by truck_number
    cursor.execute(f"""SELECT location FROM public.schmackage_logs
                   WHERE truck = {truck_number} GROUP BY max(timestamp) LIMIT 1""")
    truck_report_data['last_delivery'] = cursor.fetchall()
    return truck_report_data


# Finds the best customer in the past year
def most_frequent_customer():
    cursor.execute("""SELECT shipper_id FROM public.schmackage_logs, public.schmackages
                   WHERE public.schmackage_logs.tracking_number = public.schmackages.tracking_number AND
                   DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -12, getdata()))
                   GROUP BY shipper_id ORDER BY price LIMIT 1;""")
    return cursor.fetchone()


# Finds the customer that spent the most money in the past year
def most_spent_customer():
    cursor.execute("""SELECT shipper_id FROM public.schmackages
                   WHERE DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -12, getdata()))
                   GROUP BY shipper_id ORDER BY SUM(price) LIMIT 1;""")
    return cursor.fetchone()


# Bills the customers for the past moneth of services
# 'simple_bill' => customer, address, and amount owed
# 'itemized_bill' => bill listing each individual shipment and the chargest for it
def bill_customers(bill_type='simple_bill'):
    # Creates simple bill
    if bill_type == 'simple_bill':
        cursor.execute("""SELECT shipper_id, return_address, SUM(price) FROM public.schmackages
                       WHERE DATEPART(m, timestamp) = DATEPART(m, DATEADD(m, -1, getdate())) AND
                       DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -1, getdate()))
                       GROUP BY shipper_id""")

    # Gives bill type by item
    elif bill_type == 'itemized_bill':
        cursor.execute("""SELECT shipper_id, tracking_number, price FROM public.schmackages, public.schmackage_logs
                       WHERE public.schmackages.tracking_number = public.schmackage_logs.tracking_number AND
                       DATEPART(m, timestamp) = DATEPART(m, DATEADD(m, -1, getdate())) AND
                       DATEPART(yyyy, timestamp) = DATEPART(yyyy, DATEADD(m, -1, getdate()))
                       GROUP BY shipper_id""")

    # Returns error message when invalid bill type is used
    else:
        return '''Error, invalid bill_type, please choose "simple_bill" or "itemized_bill"'''
    return cursor.fetchall()


# Generates random schmucks
def gen_schmucks(num):
    for x in range(num):
        address = gen_string()
        email = gen_string()
        phone = gen_string(10)
        payment_info = gen_string()
        subscrimer = gen_bool()
        cursor.execute(f"""INSERT INTO public.schmucks(shipper_id, address, email, phone, payment_info, crime_subscrimer)
                       VALUES (gen_random_uuid(), '{address}', '{email}', '{phone}', '{payment_info}', {subscrimer});""")

CREATE_TABLES = [

    '''
    CREATE TABLE IF NOT EXISTS public.schmucks
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
        OWNER to postgres;''',

    '''
    CREATE TYPE package_status AS ENUM ('label printed at', 'departed from', 'arrived at', 'out for delivery', 'delivered');
    CREATE TABLE IF NOT EXISTS public.schmackage_logs
    (
        tracking_number integer NOT NULL,
        "timestamp" timestamp with time zone NOT NULL,
        shipping_status character varying COLLATE pg_catalog."default",
        location character varying COLLATE pg_catalog."default",
        truck integer,
        CONSTRAINT schmackage_logs_pkey PRIMARY KEY (tracking_number, "timestamp")
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.schmackage_logs
        OWNER to postgres;''',

    '''

    CREATE TYPE package_type AS ENUM ('flat envelope', 'small box', 'large box', 'crate', 'barrel', 'keg');
    CREATE TYPE package_priority AS ENUM ('dandelion tuft', 'phony express', 'standard', 'priority', 'first class', 'ludicrous speed', 'ethically and morally unsound speed');
    CREATE TABLE IF NOT EXISTS public.schmackages
    (
        tracking_number integer NOT NULL,
        address character varying(128) COLLATE pg_catalog."default" NOT NULL,
        return_address character varying(128) COLLATE pg_catalog."default" NOT NULL,
        package_weight double precision NOT NULL,
        ptype package_type NOT NULL;
        priority package_priority NOT NULL;
        shipping_considerations character varying(256) COLLATE pg_catalog."default",
        price double precision NOT NULL,
        CONSTRAINT schmackages_pkey PRIMARY KEY (tracking_number)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.schmackages
        OWNER to postgres;'''

]

RESET_DB = [
    "DROP TABLE IF EXISTS public.schmucks;",
    "DROP TABLE IF EXISTS public.schmackage_logs;",
    "DROP TABLE IF EXISTS public.schmackages;"
]

GENERATE_TEST_DATA = [
    """INSERT INTO public.schmucks(shipper_id, address, email, phone, payment_info, crime_subscrimer) VALUES (
        gen_random_uuid(), '101 N College Ave, Annville, PA 17003', 'yarnall@lvc.edu', '7171008995', '1234567890ABCDEFG', FALSE
    );"""
]

def reset_db(cursor):
    map(cursor.execute, RESET_DB)
    map(cursor.execute, CREATE_TABLES)



if __name__ == "__main__":
    #Forms connection and sets up cursor to access SHAMAZON
    conn = p.connect(database = 'shamazon',
                     host = 'localhost',
                     user = input('Username: '),
                     password = input('Password: '),
                     port = '5432')
    cursor = conn.cursor()

    reset_db(cursor)
    gen_schmucks(1000)
    cursor.execute("SELECT * FROM schmucks")
    print(cursor.fetchall())
