from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("dept_name").orderBy(F.col("is_legal_2_per_budget_filled").desc(), "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Marketing"))))
    .withColumn("is_legal", F.when(F.col("dept_name") == "Legal", 1).otherwise(0))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("location_filled", F.coalesce("location", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_filled",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal_2"),
    )
)

filtered = (
    cleaned
    .filter(F.col("dept_name").eqNullSafe(F.lit("Engineering")))
    .select(
        "*",
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
        (F.col("is_legal_2") / F.nullif("budget_filled", F.lit(0))).alias("is_legal_2_per_budget_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("budget_filled_avg_6_prior", F.avg("budget_filled").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "is_legal_2",
        "dept_name_location",
        "is_legal_2_per_budget_filled",
        "budget_filled_avg_6_prior",
        "seq_no",
        F.when(F.col("location").rlike("^(Chiang Mai|London)$"), "PRIORITY").when(F.col("location").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_A").alias("location_zone"),
    )
    .orderBy(F.col("seq_no").desc_nulls_last(), "is_legal_2_per_budget_filled")
)
