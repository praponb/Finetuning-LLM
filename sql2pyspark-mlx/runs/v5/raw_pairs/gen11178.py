from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget_int").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc(), "id")
w4 = Window.partitionBy("location").orderBy(F.col("budget_int").desc(), "id")
w5 = Window.partitionBy("dept_name", "location").orderBy(F.col("cumulative_budget_int").asc_nulls_last())
w6 = Window.partitionBy("dept_name")
w7 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget_int").desc(), "id")
w8 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_min_cumulative_budget_int").desc())
w9 = Window.partitionBy("dept_name").orderBy(F.col("row_seq").asc_nulls_last())
w10 = Window.partitionBy("dept_name").orderBy(F.col("budget_int").asc_nulls_first())

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "New York"]) & F.col("dept_name").isNotNull())
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .filter(F.col("location").isin(["London", "Bangkok", "Chiang Mai"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget_int",
        "dept_name_clean",
        F.row_number().over(w).alias("row_seq"),
        F.sum("budget_int").over(w2).alias("cumulative_budget_int"),
        F.ntile(3).over(w3).alias("budget_filled_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows_departments = (
    latest
    .select(
        "id",
        "dept_name",
        "budget_int",
        "row_seq",
        "budget_filled_bucket",
        F.dense_rank().over(w5).alias("cumulative_budget_int_dense_rank"),
        F.min("cumulative_budget_int").over(w6).alias("dept_name_min_cumulative_budget_int"),
        F.ntile(3).over(w7).alias("cumulative_budget_int_bucket"),
    )
)

ranked = (
    with_windows_departments
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

remaining_keys = (
    ranked
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 2273000)
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "budget_int",
        "row_seq",
        "dept_name_min_cumulative_budget_int",
        F.rank().over(w9).alias("row_seq_rank"),
        F.round((F.col("cumulative_budget_int_dense_rank") / F.sum("cumulative_budget_int_dense_rank").over(w6)), 4).alias("cumulative_budget_int_dense_rank_share"),
        F.round((F.col("cumulative_budget_int_dense_rank") / F.sum("cumulative_budget_int_dense_rank").over(w6)), 4).alias("cumulative_budget_int_dense_rank_share_2"),
        F.cume_dist().over(w10).alias("budget_int_cume_dist"),
        F.max("dept_name_min_cumulative_budget_int").over(w6).alias("dept_name_max_dept_name_min_cumulative_budget_int"),
    )
)
