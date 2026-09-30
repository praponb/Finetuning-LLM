from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w2 = Window.orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

valid = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("segment") != "SMB"))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.trim("country").alias("clean_country"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.plan") == "standard"), "left_anti")
)

metrics = (
    kept
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.avg("age").over(w2).alias("age_avg_6"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

kept_customers = (
    metrics
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.amount") <= 600), "left_anti")
)

result = (
    kept_customers
    .groupBy(
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.max("age").alias("max_age"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age"))).alias("corporate_total_age"),
        F.avg(F.when(F.col("country") == "US", F.col("credit_limit"))).alias("us_avg_credit_limit"),
        F.round(F.sum("age_avg_6") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("age_avg_6_per_smb"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("row_seq_per_consumer"),
    )
)
