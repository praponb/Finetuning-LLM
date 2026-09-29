from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc())

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-03-14").cast("date")) & (F.col("product_id") == 2))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "sale_date", "units", "revenue_per_unit")
)
