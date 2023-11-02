# shamazon-crime
Implementation of a package-sending company's database for a Database class.


## Shamazon Official Thesaurus:
  -packages = schmackages      *#Individual package information*
 
  -package_logs = schmackage_logs      *#Tracking updates*
  
  -customers/shippers = schmucks      *#Customer information*



## Functions (see functions in code for needed input types):
  -add_schmuck() to add 1 schmuck
  
  -add_schmackage() to add 1 schmackage
  
  -add_log() to add 1 schmackage_log


## Client Queries (for the given queries in the project packet)
  -truck_crash_report(truck_number)

  -most_frequent_customer()

  -most_spent_customer()

  -bill_customers() *# Default generates simple bill, 'itemized_bill' generates itemized bill, 'type' organizes bills by payment type*



## Given Test Data Generation: (for complete data, call gen_schmucks() and gen_schmackages() which will fill all 3 tables)
  -gen_schmucks(number of schmucks)
  
  -gen_schmackages_stuff(number of schmackages)      *# Also generates 1 to 10 schmackage logs per schmackage*
  
  -reset_db(cursor)      *# Clears all existing data in each table*

## To run:
All functions are in the client_queries.py file. Just call whatever functions from above in main (very bottom of the file) and run.
