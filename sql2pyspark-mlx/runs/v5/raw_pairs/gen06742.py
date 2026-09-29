from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel") != "store")
    .select("e.id", "e.order_id", "e.shipped_date", "e.cost", F.col("o.amount"))
)

matched_keys = (
    with_dims
    .select("id")
    .intersectAll(
        df_shipments
        .filter(F.year("delivered_date") >= 2023)
        .select("id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "order_id", "cost", "amount")
    .orderBy(F.col("cost").desc_nulls_first(), "amount")
)
