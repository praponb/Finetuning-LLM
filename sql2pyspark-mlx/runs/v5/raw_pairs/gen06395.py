from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("Kerry"))) & (F.col("shipped_date") < "2024-05-01"))
    .select(
        "id",
        "cost",
        "status",
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_shipments = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

base_shipments = (
    latest_shipments
    .filter(F.col("status") == "LOST")
    .select(
        "id",
        "cost",
        "status",
        "latest_rank",
        "rank_in_group",
        F.least("latest_rank", "rank_in_group").alias("least_latest_rank_rank_in_group"),
    )
)

metrics = (
    base_shipments
    .select(
        "id",
        "status",
        "latest_rank",
        "rank_in_group",
        F.avg("cost").over(w3).alias("running_avg_cost"),
        F.max("cost").over(w3).alias("running_max_cost"),
        F.count("cost").over(w3).alias("running_count_cost"),
    )
)

result = (
    metrics
    .cube(
        "status",
        F.when(F.col("running_count_cost") >= 15, "HIGH").otherwise("LOW").alias("running_count_cost_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.min("running_avg_cost").alias("min_running_avg_cost"),
    )
    .filter(F.col("delivered_count") < 1000)
    .orderBy(F.col("delivered_count").desc())
)
