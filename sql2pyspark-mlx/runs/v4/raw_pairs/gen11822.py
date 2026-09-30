from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") < 230)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

scored = (
    valid
    .withColumn("unit_price_avg_6_prior", F.avg("unit_price").over(w))
    .withColumn("unit_price_avg_2", F.avg("unit_price").over(w2))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select("id", "order_id", "product_id", "unit_price", "unit_price_avg_2")
    .orderBy(F.col("unit_price").desc_nulls_first(), F.col("order_id").desc())
)
