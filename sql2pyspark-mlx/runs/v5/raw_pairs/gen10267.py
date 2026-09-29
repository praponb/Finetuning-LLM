from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-04-23"))) & F.month("shipped_date").isin([9, 10, 11]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("carrier", ["Kerry", "UPS"])
    .agg(F.sum("cost").alias("value_sum"))
    .select("status", F.col("Kerry").alias("kerry"), F.col("UPS").alias("ups"))
)
