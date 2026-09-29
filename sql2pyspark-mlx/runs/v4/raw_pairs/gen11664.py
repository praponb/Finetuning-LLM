from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-15"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"))
)
