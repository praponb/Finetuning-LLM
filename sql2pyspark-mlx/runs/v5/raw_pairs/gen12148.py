from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("cumulative_cost").desc_nulls_last(), "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-04-17")))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.concat("status", F.lit(" - "), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("cost_cume_dist"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "id",
        "carrier",
        "cost",
        "status_title",
        "carrier_filled",
        "cost_cume_dist",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.round((F.col("running_cost") / F.sum("running_cost").over(w3)), 4).alias("running_cost_share"),
        F.ntile(3).over(w4).alias("cost_bucket"),
    )
)

latest = (
    enriched_shipments
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("carrier")
    .agg(
        F.percentile_approx("cost_bucket", 0.95).alias("cost_bucket_p95"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
    )
    .filter(F.col("num_carriers") >= 1)
)
