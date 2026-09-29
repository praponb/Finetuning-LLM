from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w_recent = Window.partitionBy("level").orderBy(F.col("hire_date_quarter").desc_nulls_first())
w_grp = Window.orderBy(F.col("hire_date_quarter").desc(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-01-26").cast("date")) & F.col("level").isNotNull() & F.col("job_title").isin(["Engineer", "Analyst"]))
    .select(
        "id",
        "salary",
        "hire_date",
        "level",
        F.quarter("hire_date").alias("hire_date_quarter"),
    )
)

cleaned = (
    filtered
    .filter((F.col("hire_date") >= F.lit("2023-07-26").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "salary",
        "hire_date",
        "level",
        "hire_date_quarter",
        F.date_add("hire_date", 42).alias("hire_date_plus_42d"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w_recent).alias("hire_date_quarter_cume_dist"),
        F.ntile(5).over(w_grp).alias("hire_date_quarter_bucket"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("quarter", "hire_date"))
    .groupingSets(
        [["seq_no", "quarter_start"], ["seq_no"], []],
        "seq_no",
        "quarter_start",
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("hire_date_quarter_cume_dist") / F.nullif(F.count(F.when(F.col("level") == "L4", 1)), F.lit(0)), 2).alias("hire_date_quarter_cume_dist_per_l4"),
    )
)
