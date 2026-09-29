from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("days_since_shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-05").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.channel"))
)

ranked_shipments = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["order_channel", "carrier"], ["order_channel"], []],
        F.col("p.channel").alias("order_channel"),
        F.col("s.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.cost")).alias("max_cost"),
        F.max(F.col("s.days_since_shipped_date")).alias("max_days_since_shipped_date"),
    )
    .orderBy(F.col("max_cost").desc_nulls_first())
)
