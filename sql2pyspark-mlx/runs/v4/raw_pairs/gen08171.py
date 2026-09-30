from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") == 21))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.channel"), F.col("o.amount"))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .rollup(
        F.date_trunc("month", "shipped_date_plus_months").alias("activity_month"),
        "carrier",
        "channel",
    )
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
    .orderBy("web_count")
)
