from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("row_num").desc())

latest = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("dept_name").eqNullSafe(F.lit("Engineering")))
    .select(
        "*",
        F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    deduped
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 3580000)
        .select("id", "location")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("is_tokyo") >= 83, "PREMIUM").otherwise("STANDARD").alias("is_tokyo_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.percentile_approx("is_tokyo", 0.5).alias("is_tokyo_p50"),
    )
    .orderBy(F.col("is_tokyo_p50").desc())
)
