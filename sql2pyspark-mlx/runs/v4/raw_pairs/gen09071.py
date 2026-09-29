from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("city").orderBy(F.col("prev_age").desc_nulls_last(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2024-01-10").cast("date"))
    .withColumn("city_group", F.when(F.col("city").isin(["Berlin", "London"]), "Tier 1").otherwise("Other"))
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(4).over(w).alias("age_bucket"),
        F.lag("age", 2).over(w2).alias("prev_age"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank_2", F.row_number().over(w4))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

result = (
    ranked
    .withColumn("prev_age_share", F.round((F.col("prev_age") / F.sum("prev_age").over(w5)), 4))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w8))
)
