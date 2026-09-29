from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("order_id").orderBy("cost_int", "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-11-21").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM-dd"))
    .withColumn("cost_int", F.col("cost").cast("int"))
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cleaned = (
    deduped
    .filter(F.col("shipped_date") > F.lit("2023-04-03").cast("date"))
    .select(
        "*",
        F.round("cost", 2).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.order_date"))
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "cost_int",
        "cost_rounded",
        "carrier_filled",
    )
    .orderBy("id", F.col("cost_rounded").desc())
)
