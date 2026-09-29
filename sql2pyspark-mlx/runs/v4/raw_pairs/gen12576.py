from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first(), "id")

latest = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(F.date_trunc("quarter", "shipped_date").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("status") == "IN_TRANSIT", F.col("cost"))).alias("in_transit_max_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
    .filter(F.col("in_transit_cost") > 0)
)
