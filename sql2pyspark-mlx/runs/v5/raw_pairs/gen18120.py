from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-04-03")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

valid = (
    base
    .filter(F.col("launch_date") >= F.lit("2023-03-17").cast("date"))
    .withColumn("product_name_lower", F.lower("product_name"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

filtered = (
    with_latest
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-07-05"))) & F.col("supplier_id").isNotNull())
    .withColumn("category_upper", F.upper("category"))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
)

result = (
    filtered
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "brand"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("s.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.price")), 2).alias("total_price"),
        F.grouping_id().alias("level_id"),
    )
)
