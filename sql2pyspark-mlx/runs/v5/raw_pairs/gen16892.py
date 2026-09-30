from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-08-24").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
