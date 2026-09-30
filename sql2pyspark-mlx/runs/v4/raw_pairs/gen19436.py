from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("rn").desc(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Engineering", "Legal"]))
    .withColumn("location_upper", F.upper("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    ranked
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("budget").between(1131000, 1179000))
        .select("location", "id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "dept_name", "location", "budget", "location_filled", "rn")
    .orderBy(F.col("id").desc())
)
