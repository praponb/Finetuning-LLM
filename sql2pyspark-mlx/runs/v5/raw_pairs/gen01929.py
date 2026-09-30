from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.coalesce("brand", F.lit("Unknown")).alias("brand_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

cleaned = (
    valid
    .filter((F.col("launch_date") > F.lit("2023-06-05").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "brand_filled",
        "resolved_brand",
        (F.col("price") + F.col("cost")).alias("margin"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "price",
        "cost",
        "brand_filled",
        "margin",
        F.lead("cost").over(w_running).alias("next_cost"),
        F.lead("margin").over(w_running).alias("next_margin"),
        F.lag("margin").over(w_running).alias("prev_margin"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand_filled",
        "s.margin",
        "s.next_cost",
        "s.next_margin",
        "s.seq_no",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

remaining_keys = (
    with_latest
    .select("id")
    .exceptAll(
        df_products
        .filter(F.col("brand") == "Initech")
        .select("id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "brand_filled", "margin", "seq_no", "order_item_quantity")
    .orderBy(F.col("order_item_quantity").desc_nulls_first())
)
