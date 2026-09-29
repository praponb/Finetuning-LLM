from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("adjusted_unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())
w_recent = Window.partitionBy("product_id", "status").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("unit_price") * 0.85), 2).alias("adjusted_unit_price"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id") == 14)
    .select("s.*", F.col("o.status"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims_order_items = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.launch_date"))
)

latest = (
    with_dims_order_items
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "id",
        "order_id",
        "quantity",
        "adjusted_unit_price",
        "status",
        "rn",
        F.sum("quantity").over(w_recent).alias("running_sum_quantity"),
        F.max("quantity").over(w_recent).alias("running_max_quantity"),
        F.count("quantity").over(w_recent).alias("running_count_quantity"),
        F.min("quantity").over(w_recent).alias("running_min_quantity"),
    )
)

result = (
    enriched
    .select(
        "id",
        "quantity",
        "adjusted_unit_price",
        "status",
        "running_sum_quantity",
        "running_max_quantity",
        "running_count_quantity",
        "running_min_quantity",
        F.when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "GROUP_A").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("running_count_quantity").desc_nulls_last())
)
