from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    latest_match
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "quantity",
        F.when(F.col("status").rlike("^(SHIPPED|PENDING)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
)
