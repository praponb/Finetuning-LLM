from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("channel").orderBy("cost", "id")
w4 = Window.partitionBy("order_id", "channel").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.orderBy(F.col("cost").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & (F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("Unknown")))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("UNKNOWN")))
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.id", "src.order_id", "src.cost", F.col("b.channel"))
)

base = (
    joined
    .filter(F.col("order_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .select("*", F.initcap(F.trim("channel")).alias("channel_title"))
)

enriched = (
    base
    .filter(F.col("cost") <= 44)
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.rank().over(w2).alias("cost_rank"),
        F.lead("cost", 2).over(w3).alias("next_cost"),
        F.dense_rank().over(w4).alias("cost_dense_rank"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy("order_id")
    .pivot("channel", ["store", "web"])
    .agg(F.max("cost_dense_rank").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
