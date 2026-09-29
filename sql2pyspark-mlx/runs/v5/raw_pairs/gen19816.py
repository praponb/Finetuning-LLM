from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "device").orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("seq_no_2").desc_nulls_last())
w7 = Window.partitionBy("city").orderBy(F.col("running_count_seq_no_2").asc_nulls_first(), "id")

scored = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari", "Edge"]))
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.country",
        "a.first_known_country",
        "a.seq_no_2",
        "a.first_known_browser",
        F.col("c.name"),
        F.col("c.city"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "first_known_country",
        "seq_no_2",
        "name",
        "city",
        F.avg("seq_no_2").over(w5).alias("running_avg_seq_no_2"),
        F.max("seq_no_2").over(w5).alias("running_max_seq_no_2"),
        F.count("seq_no_2").over(w5).alias("running_count_seq_no_2"),
        F.rank().over(w6).alias("seq_no_2_rank"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .rollup(
        "customer_id",
        F.when(F.col("running_avg_seq_no_2") >= 6.5, "LARGE").otherwise("SMALL").alias("running_avg_seq_no_2_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("seq_no_2")).otherwise(0)).alias("tokyo_seq_no_2"),
        F.round(F.sum("seq_no_2") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("seq_no_2_per_mobile"),
        F.round(F.avg("running_count_seq_no_2"), 2).alias("avg_running_count_seq_no_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("avg_running_count_seq_no_2").desc_nulls_last())
)
