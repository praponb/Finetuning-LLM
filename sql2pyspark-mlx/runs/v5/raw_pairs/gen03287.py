from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_customers
    .select(
        "id",
        "email",
        "segment",
        "is_active",
        F.count("credit_limit").over(w).alias("country_count_credit_limit"),
        F.ntile(4).over(w2).alias("credit_limit_bucket"),
        F.sum("credit_limit").over(w3).alias("cumulative_credit_limit"),
    )
)

result = (
    enriched
    .rollup("segment")
    .agg(
        F.percentile_approx("credit_limit_bucket", 0.95).alias("credit_limit_bucket_p95"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
)
