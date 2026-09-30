from pyspark.sql import functions as F

result = df_stores.select("city", F.last_day("opened_date").alias("opened_date_month_end"))
