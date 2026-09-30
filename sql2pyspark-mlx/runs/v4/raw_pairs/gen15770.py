from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first(), "id")

deduped = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("carrier", "status")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
    )
)
