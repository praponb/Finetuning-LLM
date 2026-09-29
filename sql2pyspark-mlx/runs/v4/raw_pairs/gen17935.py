from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash", "Kerry"]) & (F.col("status") != "DELIVERED"))
    .withColumn("delivered_date_plus_months", F.add_months("delivered_date", 6))
)

delivered_quarter_summary = (
    src
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = delivered_quarter_summary
