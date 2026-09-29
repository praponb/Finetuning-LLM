from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-09-17").cast("date")) & F.col("store_id").isNotNull())
    .withColumn("is_web", F.when(F.col("channel") == "web", 1).otherwise(0))
)

result = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        F.col("c.delivered_date"),
    )
)
