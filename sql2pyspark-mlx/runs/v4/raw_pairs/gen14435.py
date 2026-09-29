from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("val_tier").orderBy(F.col("pro_val").desc())

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

result = (
    lines
    .groupBy(
        F.when(F.col("val") >= 62, "LARGE").otherwise("SMALL").alias("val_tier"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("val")).otherwise(0)).alias("pro_val"),
        F.sum(F.when(F.col("plan") == "pro", F.col("val")).otherwise(0)).alias("pro_val_2"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count("*").alias("event_count"),
    )
    .withColumn("pro_val_rank", F.rank().over(w))
)
