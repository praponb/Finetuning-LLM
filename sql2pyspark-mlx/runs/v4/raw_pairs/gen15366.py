from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-12").cast("date")) & (F.col("cost") < 190))
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("price").over(w).alias("cumulative_price"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

enriched = (
    latest_match
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.supplier_name"))
)

wide = (
    enriched
    .groupBy("supplier_id")
    .pivot("brand", ["Globex", "Acme"])
    .agg(F.sum("cost_percent_rank").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "supplier_id",
        F.col("Globex_amount").alias("globex_amount"),
        F.col("Globex_n").alias("globex_n"),
        F.col("Acme_amount").alias("acme_amount"),
        F.col("Acme_n").alias("acme_n"),
    )
)

result = wide.select("supplier_id", "globex_n", "acme_n")
