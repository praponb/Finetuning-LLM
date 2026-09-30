from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("channel").orderBy("running_count_amount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("amount_times_running_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id")

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "*",
        F.dayofweek("order_date").alias("order_date_dayofweek"),
        F.lower("channel").alias("channel_lower"),
    )
)

scored = (
    cleaned
    .filter(F.col("order_date_dayofweek") == 51)
    .withColumn("running_avg_amount", F.avg("amount").over(w_grp))
    .withColumn("running_count_amount", F.count("amount").over(w_grp))
    .withColumn("running_min_amount", F.min("amount").over(w_grp))
)

cleaned_orders = (
    scored
    .filter(F.col("order_date") > F.lit("2023-09-18").cast("date"))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "channel_lower",
        "running_avg_amount",
        "running_count_amount",
        (F.col("amount") * F.col("running_count_amount")).alias("amount_times_running_count_amount"),
        F.col("running_min_amount").cast("int").alias("running_min_amount_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

latest_match = (
    cleaned_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

result = (
    latest_match
    .select(
        "id",
        "store_id",
        "discount",
        "shipping_fee",
        "channel_lower",
        "running_avg_amount",
        "running_count_amount",
        "running_min_amount_int",
        "resolved_channel",
        "order_item_unit_price",
        F.avg("order_item_unit_price").over(w3).alias("order_item_unit_price_avg_2"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
        F.min("running_min_amount_int").over(w5).alias("store_id_min_running_min_amount_int"),
    )
)
