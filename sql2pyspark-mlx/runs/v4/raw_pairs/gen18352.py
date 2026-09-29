from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("cost_whole").desc(), "id")
w3 = Window.orderBy("delivered_date", "id")
w4 = Window.partitionBy("carrier").orderBy("cost_whole", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("order_id").orderBy("cost_whole", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_whole_avg_3_prior").desc_nulls_first())
w7 = Window.partitionBy("order_id").orderBy("cost_whole_avg_3_prior", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier", "status").orderBy("cost_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("carrier")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("status") == "IN_TRANSIT"))
    .select(
        "*",
        F.when(F.col("cost") >= 100, "High").when(F.col("cost") >= 29, "Medium").otherwise("Low").alias("cost_level"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

valid = (
    base
    .filter(F.col("shipped_date") > F.to_date(F.lit("2024-03-20")))
    .select(
        "*",
        F.ceil("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean_2"),
    )
)

with_windows = (
    valid
    .withColumn("last_known_cost_whole", F.last("cost_whole", ignorenulls=True).over(w))
    .withColumn("cost_whole_bucket", F.ntile(5).over(w2))
    .withColumn("prev_cost_whole", F.lag("cost_whole", 1).over(w3))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w4))
    .withColumn("cost_whole_avg_3_prior", F.avg("cost_whole").over(w5))
)

scored = (
    with_windows
    .select(
        "*",
        F.rank().over(w6).alias("cost_whole_avg_3_prior_rank"),
        F.avg("cost_whole_avg_3_prior").over(w7).alias("cost_whole_avg_3_prior_avg_6_prior"),
    )
)

scored_shipments = (
    scored
    .select(
        "*",
        F.sum("cost_whole").over(w8).alias("running_cost_whole"),
        F.sum("cost_whole_avg_3_prior_avg_6_prior").over(w_ordered).alias("running_cost_whole_avg_3_prior_avg_6_prior"),
        F.round((F.col("cost") / F.sum("cost").over(w10)), 4).alias("cost_share"),
    )
)

with_dims = (
    scored_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.order_date"), F.col("o.channel"))
)

result = (
    with_dims
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "carrier_clean",
        "cost_whole",
        "carrier_clean_2",
        "last_known_cost_whole",
        F.when(F.col("status").rlike("^(RETURNED|IN_TRANSIT)$"), "EXTENDED").when(F.col("status").rlike("^(DELIVERED|LOST)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("order_id").desc(), "status_zone")
)
