from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date_plus_months").asc_nulls_first())

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-11-18"))) & F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.add_months("sale_date", 1).alias("sale_date_plus_months"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
