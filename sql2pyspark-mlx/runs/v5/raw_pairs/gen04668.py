from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("category").orderBy("tags_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("launch_date", "id")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags_count",
        "brand_clean",
        F.max("tags_count").over(w).alias("supplier_id_max_tags_count"),
        F.first("brand", ignorenulls=True).over(w2).alias("first_known_brand"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
