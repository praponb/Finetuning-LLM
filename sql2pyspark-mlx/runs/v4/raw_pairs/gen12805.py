from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w_recent = Window.partitionBy("brand").orderBy("cost", "id")

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2024-03-04"))) & F.col("supplier_id").isNotNull() & (F.col("stock_qty") > 313))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.dayofweek("launch_date").alias("launch_date_dayofweek"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.ntile(5).over(w_recent).alias("cost_bucket"),
    )
)
