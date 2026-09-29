from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("shipping_fee").desc(), "id")

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-06").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
