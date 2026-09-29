from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)
