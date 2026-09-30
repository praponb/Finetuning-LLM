from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id")
w_running = Window.partitionBy("city").orderBy("rating", "id")

scored = (
    df_suppliers
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w_running).alias("rating_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("rating_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("rating_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("row_seq")).otherwise(0)).alias("chiang_mai_row_seq"),
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("seq_no")).otherwise(0)).alias("tokyo_seq_no"),
    )
    .orderBy(F.col("chiang_mai_row_seq").desc())
)
