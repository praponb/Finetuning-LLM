from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-05-08")))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "sale_date", "units", "revenue_per_unit")
)
