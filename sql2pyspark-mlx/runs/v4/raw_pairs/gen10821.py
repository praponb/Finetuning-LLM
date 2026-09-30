from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("cost") < 29) & (F.col("status") == "LOST")))
    .withColumn("shipped_date_plus_89d", F.date_add("shipped_date", 89))
    .withColumn("cost_tier", F.when(F.col("cost") >= 98, "High").when(F.col("cost") >= 34, "Medium").otherwise("Low"))
    .orderBy("carrier")
)
