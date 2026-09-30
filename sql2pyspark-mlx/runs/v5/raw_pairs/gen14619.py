from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("channel").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id", "channel")

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-08-14").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "order_date", "channel", "amount")
)

filtered = (
    latest
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("amount") < 1440))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.when(F.col("channel") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)

metrics = (
    filtered
    .filter(F.col("is_mobile") <= 30)
    .select(
        "id",
        "order_date",
        "amount",
        "channel_prefix",
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.sum("amount").over(w5).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share_2"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNotNull()), "left_semi")
)

result = (
    kept
    .select("id", "amount_percent_rank", F.col("amount").alias("AMOUNT"))
    .unpivot(
        ids=["id"],
        values=["amount_percent_rank", "AMOUNT"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
