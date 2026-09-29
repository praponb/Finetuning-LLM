from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("city")
w_ordered_2 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment", "city").orderBy(F.col("city_stddev_samp_adjusted_age").desc(), "id")
w5 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2024-01-21").cast("date")) & (F.col("country") == "US"))
    .select(
        "*",
        F.round((F.col("age") * 0.85), 2).alias("adjusted_age"),
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .select(
        "*",
        F.corr("age", "credit_limit").over(w_ordered).alias("corr_age_credit_limit"),
        F.row_number().over(w_ordered_2).alias("seq_no"),
        F.stddev_samp("adjusted_age").over(w_ordered).alias("city_stddev_samp_adjusted_age"),
    )
)

cleaned = (
    enriched
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-11-11")))
    .withColumn("segment_title", F.initcap(F.trim("segment")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

deduped_customers = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped_customers
    .select(
        "*",
        F.last("adjusted_age", ignorenulls=True).over(w5).alias("last_known_adjusted_age"),
        F.sum("credit_limit").over(w6).alias("cumulative_credit_limit"),
    )
)

pivoted = (
    scored
    .groupBy("city")
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.sum("adjusted_age"))
    .select(
        "city",
        F.col("SMB").alias("smb"),
        F.col("Consumer").alias("consumer"),
        F.col("Corporate").alias("corporate"),
    )
)

result = pivoted.select("city", "smb", "consumer", "corporate")
