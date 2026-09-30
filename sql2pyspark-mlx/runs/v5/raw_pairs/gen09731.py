from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("dept_name_length", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") <= 2397000))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("dept_name_length", F.length("dept_name"))
)

metrics = (
    base
    .filter(F.col("dept_name_length").between(23, 61))
    .select(
        "id",
        "location",
        "budget",
        "dept_name_lower",
        F.max("dept_name_length").over(w).alias("last7_max_dept_name_length"),
        F.min("dept_name_length").over(w).alias("last7_min_dept_name_length"),
        F.sum("dept_name_length").over(w).alias("last7_sum_dept_name_length"),
    )
)

valid = (
    metrics
    .filter((~F.col("location").eqNullSafe(F.lit("London"))) & (F.col("last7_sum_dept_name_length") < 200))
    .select(
        "*",
        F.when(F.col("location").isin(["Tokyo", "Berlin"]), "Priority").otherwise("Other").alias("location_group"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

base_departments = (
    valid
    .filter(F.col("last7_max_dept_name_length") >= 26)
    .withColumn("budget_rounded", F.round("budget", 2))
    .withColumn("last7_sum_dept_name_length_whole", F.ceil("last7_sum_dept_name_length"))
)

kept_keys = (
    base_departments
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("location") == "Berlin")
        .select("location", "id")
    )
)

verified = (
    base_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "location",
        "dept_name_lower",
        "last7_max_dept_name_length",
        "last7_sum_dept_name_length",
        "location_group",
        "location_filled",
        "last7_sum_dept_name_length_whole",
        F.when(F.col("location").rlike("^(Tokyo|Bangkok)$"), "PRIORITY").otherwise("GROUP_A").alias("location_zone"),
    )
    .orderBy(F.col("last7_sum_dept_name_length").desc_nulls_last())
)
