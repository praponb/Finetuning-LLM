from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id")

with_windows = (
    df_products
    .withColumn("next_price", F.lead("price", 1).over(w))
    .withColumn("stock_qty_bucket", F.ntile(3).over(w2))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.supplier_name"))
)

base = (
    enriched
    .filter(F.col("launch_date") >= F.lit("2024-01-08").cast("date"))
    .select(
        "*",
        F.date_sub("launch_date", 9).alias("launch_date_minus_9d"),
        F.when(F.col("next_price") >= 1000, "High").when(F.col("next_price") >= 380, "Medium").otherwise("Low").alias("next_price_band"),
    )
)

result = (
    base
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.when(F.col("brand").rlike("^(Acme|Globex)$"), "CORE").when(F.col("brand").rlike("^(Initech|Umbrella)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("brand_zone"),
    )
    .orderBy(F.col("price").desc())
)
