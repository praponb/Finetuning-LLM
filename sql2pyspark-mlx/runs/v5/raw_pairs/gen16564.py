from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").desc_nulls_first())

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > F.to_date(F.lit("2023-05-20"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
    )
)

valid_shipments = (
    valid
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-09-25"))) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "cost_scaled",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

base = (
    valid_shipments
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["Flash", "UPS"]))
    .select(
        "*",
        (F.col("cost_scaled") * F.col("cost")).alias("cost_scaled_times_cost"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT"])
    .agg(F.avg("cost").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
