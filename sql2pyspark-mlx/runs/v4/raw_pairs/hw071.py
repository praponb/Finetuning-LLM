from pyspark.sql import functions as F

result = df_employees.select("id", F.concat("full_name", F.lit(" <"), "email", F.lit(">")).alias("contact"))
