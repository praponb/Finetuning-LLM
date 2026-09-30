from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")
w2 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("category").orderBy("quantity", "id")
w5 = Window.partitionBy("brand").orderBy("price_avg_5", "id")

valid = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-08-23")))
    .withColumn("margin", (F.col("price") + F.col("cost")))
    .withColumn("is_premium", F.array_contains("tags", "premium"))
)

metrics = (
    valid
    .filter(~F.col("category").isin(["Books", "Electronics", "Fashion"]))
    .select(
        "*",
        F.lead("margin", 1).over(w).alias("next_margin"),
        F.avg("price").over(w2).alias("price_avg_5"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

with_windows = (
    latest_match
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "next_margin",
        "quantity",
        F.lead("price_avg_5").over(w4).alias("next_price_avg_5"),
        F.lag("cost").over(w4).alias("prev_cost"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    with_windows
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.row_seq")).alias("max_row_seq"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
