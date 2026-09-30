from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"), "carrier")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") > 40)
    .orderBy(F.col("min_cost").desc())
)
