from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-12-25", "2024-10-01") & F.col("delivered_date").between("2023-09-01", "2024-02-01") & F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .groupBy("carrier", F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
    .orderBy(F.col("delivered_year").desc(), F.col("pct_lost").desc())
)
