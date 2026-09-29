from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = verified.select("id", "location", "budget")
