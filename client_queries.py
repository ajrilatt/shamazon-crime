import psycopg2 as p

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

]

RESET_DB = [
    "DROP TABLE IF EXISTS public.schmucks;",
    "DROP TABLE IF EXISTS public.schmackage_logs;",
    "DROP TABLE IF EXISTS public.schmackages;"
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

    #Returns all packages
    cursor.execute("SELECT * FROM schmackages")
    print(cursor.fetchone())
