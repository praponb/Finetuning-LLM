from pyspark.sql import functions as F

src = df_shipments.filter(F.year("delivered_date") == 2024)

agg = (
    src
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    agg
    .filter(F.col("pct_returned") >= 78)
    .select("carrier", "pct_returned", "max_cost")
    .orderBy(F.col("pct_returned").desc())
)
