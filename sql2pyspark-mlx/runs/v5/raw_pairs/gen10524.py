from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter(F.col("cost").between(500, 580) & (F.col("launch_date") > F.lit("2023-10-02").cast("date")))
    .select(
        "*",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") < 160), "left_anti")
)

base = (
    not_flagged
    .filter((F.col("launch_date") > F.lit("2023-03-29").cast("date")) & F.col("brand").isNotNull())
    .select(
        "*",
        F.datediff("launch_date", "launch_date_plus_months").alias("days_launch_date_plus_months_to_launch_date"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([3, 21, 29])), "left_anti")
)

result = (
    eligible
    .groupBy("brand")
    .pivot("category", ["Sports", "Books"])
    .agg(F.sum("stock_qty").alias("total"), F.count(F.lit(1)).alias("n"))
)
