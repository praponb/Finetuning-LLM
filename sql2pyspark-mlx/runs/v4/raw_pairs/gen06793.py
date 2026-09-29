from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("location") != "Tokyo")
        .select("location", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "dept_name",
        "location",
        F.when(F.col("location").rlike("^(London|Tokyo)$"), "GROUP_B").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("EXTENDED").alias("location_zone"),
    )
    .orderBy(F.col("location").desc(), F.col("dept_name").desc())
)
