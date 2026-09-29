from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-01", "2023-11-03"))
    .select(
        "cost",
        F.date_sub("shipped_date", 90).alias("shipped_date_minus_90d"),
        F.when(F.col("cost") >= 97, "High").when(F.col("cost") >= 16, "Medium").otherwise("Low").alias("cost_band"),
    )
    .orderBy(F.col("cost").desc())
)
