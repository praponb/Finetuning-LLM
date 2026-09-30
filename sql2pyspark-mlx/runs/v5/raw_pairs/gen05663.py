from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.orderBy(F.col("sqft").desc_nulls_last())
w6 = Window.orderBy("next_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("region").orderBy("quantity", "id")
w_grp = Window.orderBy("sqft_share", "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

ranked = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .select(
        "*",
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.row_number().over(w2).alias("seq_no"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

scored = (
    with_latest
    .filter(F.col("next_sqft") < 13207)
    .select(
        "*",
        F.percent_rank().over(w5).alias("sqft_percent_rank"),
        F.last("sqft_share", ignorenulls=True).over(w6).alias("last_known_sqft_share"),
        F.row_number().over(w_hist).alias("seq_no_2"),
        F.ntile(4).over(w_grp).alias("sqft_share_bucket"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"), F.col("c.order_date"))
)

result = (
    with_last
    .groupBy("city")
    .pivot("region", ["Central", "West"])
    .agg(F.max("sqft").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "city",
        F.col("Central_value_sum").alias("central_value_sum"),
        F.col("Central_cnt").alias("central_cnt"),
        F.col("West_value_sum").alias("west_value_sum"),
        F.col("West_cnt").alias("west_cnt"),
    )
)
