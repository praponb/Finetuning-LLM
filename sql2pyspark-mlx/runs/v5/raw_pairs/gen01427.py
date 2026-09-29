from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit("/"), "location"))
    .withColumn("location_prefix", F.substring("location", 1, 3))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "dept_name_location", "location_prefix")
)

cleaned = (
    latest
    .filter(F.col("dept_name") == "Legal")
    .select("*", F.upper("dept_name").alias("dept_name_upper"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("location") == "Singapore")
        .select("id", "dept_name")
    )
)

verified = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy("dept_name")
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count_2"),
    )
)

result = (
    stats
    .select(
        "dept_name",
        "num_dept_names",
        "hr_count",
        F.when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "CORE").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name").desc(), "hr_count")
)
