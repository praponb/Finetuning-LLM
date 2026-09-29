from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_first(), "id")

cleaned = (
    df_products
    .filter(F.col("cost") <= 310)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
