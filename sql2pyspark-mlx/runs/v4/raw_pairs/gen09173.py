from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")

latest = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    latest
    .filter(F.col("sale_date") > "2025-04-22")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = deduped.select("id", "store_id", "sale_date", "units", "revenue", "rank_in_group")
