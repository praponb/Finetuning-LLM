from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-06-13").cast("date")) & F.col("product_id").isin([6, 7, 9, 10]))
    .select(
        "id",
        "product_id",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.product_name"))
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select("id", "product_id", "revenue", "revenue_per_unit")
    .orderBy("id")
)
