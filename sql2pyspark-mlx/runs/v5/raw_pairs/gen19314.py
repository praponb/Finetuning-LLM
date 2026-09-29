from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-04")))
    .select("*", F.round("revenue", 0).alias("revenue_rounded"))
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "sale_date", "revenue_rounded")
)
