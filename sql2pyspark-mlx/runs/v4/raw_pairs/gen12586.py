from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)
