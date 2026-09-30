from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("%Ford%")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("location").rlike("^(New York|London)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc_nulls_first(), F.col("id").desc())
    .limit(5)
)
