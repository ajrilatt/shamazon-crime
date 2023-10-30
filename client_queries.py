import psycopg2 as p

BIG_ASS_STRING1 = '''-- DROP TABLE IF EXISTS public.schmucks;

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
    OWNER to postgres;'''
BIG_ASS_STRING4 = '''-- DROP TABLE IF EXISTS public.schmucks;

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
    OWNER to postgres;'''
BIG_ASS_STRING3 = '''-- DROP TABLE IF EXISTS public.schmucks;

CREATE TABLE IF NOT EXISTS public.schmackages
(
    tracking_number integer NOT NULL,
    address character varying(128) COLLATE pg_catalog."default" NOT NULL,
    return_address character varying(128) COLLATE pg_catalog."default" NOT NULL,
    package_weight double precision NOT NULL,
    shipping_considerations character varying(256) COLLATE pg_catalog."default",
    price double precision NOT NULL,
    CONSTRAINT schmackages_pkey PRIMARY KEY (tracking_number)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.schmackages
    OWNER to postgres;'''


#Finds the best customer in the past year
def most_frequent_customer():
    cursor.execute("""SELECT shipper_id FROM schmackage_logs, schmackages
                   WHERE schmackage_logs.tracking_number = schmackages.tracking_number
                   GROUP BY shipper_id ORDER BY price LIMIT 1;"""
    return cursor.fetchone()

#Finds the customer that spent the most money in the past year
def most_spent_customer():
    cursor.execute("""SELECT shipper_id FROM schmackages
                   GROUP BY shipper_id ORDER BY SUM(price) LIMIT 1;"""
    return cursor.fetchone()

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
