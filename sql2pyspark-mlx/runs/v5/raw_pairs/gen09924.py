from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)
