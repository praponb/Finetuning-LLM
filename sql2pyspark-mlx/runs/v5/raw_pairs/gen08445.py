from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("days_since_launch_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("brand").orderBy("price", "id")
w4 = Window.partitionBy("brand", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-24").cast("date"))
    .withColumn("cost_whole", F.floor("cost"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.launch_date",
        "s.tags",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

base = (
    with_latest
    .filter(F.col("brand").isNotNull())
    .select("*", F.year("launch_date").alias("launch_date_year"))
)

base_products = (
    base
    .filter(F.col("launch_date") >= F.lit("2023-03-26").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
)

scored = (
    base_products
    .filter(F.col("days_since_launch_date") != 30)
    .withColumn("order_item_unit_price_avg_3_prior", F.avg("order_item_unit_price").over(w2))
    .withColumn("price_bucket", F.ntile(10).over(w3))
    .withColumn("cumulative_launch_date_year", F.sum("launch_date_year").over(w4))
)

result = (
    scored
    .select(
        "id",
        "brand",
        F.col("price").alias("PRICE"),
        F.col("order_item_unit_price").alias("ORDER_ITEM_UNIT_PRICE"),
    )
    .unpivot(
        ids=["id", "brand"],
        values=["PRICE", "ORDER_ITEM_UNIT_PRICE"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
