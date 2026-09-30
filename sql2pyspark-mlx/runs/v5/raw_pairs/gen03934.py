from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 89)
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)
