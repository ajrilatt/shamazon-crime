# shamazon-crime
Implementation of a package-sending company's database for a Database class.

Shamazon Official Thesaurus:
  -packages = schmackages
  -package_logs = schmackage_logs
  -customers/shippers = schmucks

Functions:
  -add_schmuck() to add 1 schmuck
  -add_schmackage() to add 1 schmackage
  -add_log() to add 1 schmackage_log

Given Test Data Generation:
  -gen_schmucks(number of schmucks)
  -gen_schmackages_stuff(number of schmackages) #also generates 1 to 10 schmackage logs per schmackage

All of our data is in the client_queries.py file.
Just call whatever queries or data generation functions you want, and if you need to clear the db
  at any point just call reset_db(cursor). Also hit the run button.
