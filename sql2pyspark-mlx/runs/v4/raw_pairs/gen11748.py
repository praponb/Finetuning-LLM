from pyspark.sql import functions as F

result = df_subscriptions.filter(F.col("start_date").between("2023-10-01", "2024-07-01"))
