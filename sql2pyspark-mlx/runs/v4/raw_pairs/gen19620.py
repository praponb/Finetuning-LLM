from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-03-20").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.date_add("order_date", 69).alias("order_date_plus_69d"))
)

result = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.cost").alias("shipment_cost"),
    )
)
