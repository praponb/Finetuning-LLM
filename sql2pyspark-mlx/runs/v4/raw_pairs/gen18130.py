from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary_whole").desc())
w_grp = Window.partitionBy("level")
w3 = Window.partitionBy("level").orderBy(F.col("level_avg_salary_whole").asc_nulls_first())
w4 = Window.orderBy(F.col("level_sum_salary_whole").desc_nulls_first())
w5 = Window.orderBy(F.col("level_avg_salary_whole_rank").desc())
w_part = Window.orderBy(F.col("salary_whole").desc_nulls_last())

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-11-28").cast("date")) & F.col("level").isNotNull() & (F.col("hire_date") < "2023-03-01"))
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "is_remote",
        "level",
        F.concat("level", F.lit(" "), "full_name").alias("level_full_name"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

cleaned = (
    filtered
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "is_remote",
        "level",
        "level_full_name",
        "resolved_level",
        F.initcap(F.trim("level")).alias("level_title"),
        F.ceil("salary").alias("salary_whole"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    latest
    .select("level", "id")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("level", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("level_avg_salary_whole", F.avg("salary_whole").over(w_grp))
    .withColumn("level_count_salary_whole", F.count("salary_whole").over(w_grp))
    .withColumn("level_sum_salary_whole", F.sum("salary_whole").over(w_grp))
)

scored_employees = (
    scored
    .withColumn("level_max_salary_whole", F.max("salary_whole").over(w_grp))
    .withColumn("level_avg_salary_whole_rank", F.rank().over(w3))
    .withColumn("level_stddev_samp_level_sum_salary_whole", F.stddev_samp("level_sum_salary_whole").over(w_grp))
    .withColumn("level_sum_salary_whole_dense_rank", F.dense_rank().over(w4))
)

scored_employees_2 = (
    scored_employees
    .withColumn("level_avg_salary_whole_rank_cume_dist", F.cume_dist().over(w5))
    .withColumn("salary_whole_dense_rank", F.dense_rank().over(w_part))
    .withColumn("level_sum_level_count_salary_whole", F.sum("level_count_salary_whole").over(w_grp))
)

totals = (
    scored_employees_2
    .groupBy(
        "level",
        F.when(F.col("level_count_salary_whole") >= 17, "PREMIUM").otherwise("STANDARD").alias("level_count_salary_whole_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "L3").alias("l3_events"),
        F.round(F.sum("salary_whole") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_whole_per_l5"),
        F.sum(F.when(F.col("level") == "L2", F.col("level_sum_salary_whole")).otherwise(0)).alias("l2_level_sum_salary_whole"),
    )
    .filter(F.col("l3_events") < 1)
)

result = (
    totals
    .select(
        "level",
        "level_count_salary_whole_tier",
        "l3_events",
        "salary_whole_per_l5",
        "l2_level_sum_salary_whole",
    )
    .orderBy("level_count_salary_whole_tier", F.col("salary_whole_per_l5").desc())
)
