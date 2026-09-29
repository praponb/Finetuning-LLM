from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_first(), "id")

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("city", "country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("age"))).alias("tokyo_total_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .filter(F.col("num_segments") >= 1)
    .orderBy(F.col("num_segments").desc())
)
