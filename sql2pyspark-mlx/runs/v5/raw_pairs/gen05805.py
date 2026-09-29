from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_products
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.last("brand", ignorenulls=True).over(w_recent).alias("last_known_brand"),
        F.rank().over(w2).alias("stock_qty_rank"),
        F.sum("price").over(w3).alias("cumulative_price"),
    )
)

result = (
    enriched
    .select(
        "id",
        "brand",
        "cost",
        "tags",
        "last_known_brand",
        "stock_qty_rank",
        "cumulative_price",
        F.when(F.col("brand").rlike("^(Globex|Umbrella)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Initech)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy(F.col("id").desc(), "cost")
)
