from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.order_date"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.row_number().over(w_grp).alias("seq_no"),
        F.first("quantity_scaled", ignorenulls=True).over(w2).alias("first_known_quantity_scaled"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    deduped
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "order_date",
        F.round("seq_no", 1).alias("seq_no_rounded"),
        (F.col("quantity") * F.col("seq_no")).alias("quantity_times_seq_no"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.launch_date"))
)

result = (
    enriched
    .select("id", "order_id", "order_date", "quantity_times_seq_no", "launch_date")
    .orderBy(F.col("order_id").desc_nulls_first())
)
