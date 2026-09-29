from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel", "region")

with_last = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.amount"),
        F.col("c.channel"),
    )
)

scored = (
    with_last
    .withColumn("sqft_bucket", F.ntile(3).over(w2))
    .withColumn("running_sqft", F.sum("sqft").over(w3))
    .withColumn("channel_avg_sqft", F.avg("sqft").over(w4))
)

eligible = (
    scored
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.revenue").isNotNull()), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "amount",
        "channel",
        "sqft_bucket",
    )
    .orderBy(F.col("sqft_bucket").desc())
)
