from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("quantity").between(110, 161))
    .withColumn("last_restock_plus_25d", F.date_add("last_restock", 25))
)

agg = (
    filtered_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = agg.select("last_restock_month", "first_last_restock", "min_last_restock")
