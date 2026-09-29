from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc_nulls_first(), "id")

deduped = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("revenue") >= 3900, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        F.date_trunc("week", "sale_date").alias("week_start"),
    )
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.count("*").alias("event_count"),
    )
)
