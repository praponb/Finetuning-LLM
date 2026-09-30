from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

base = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.concat_ws("/", "dept_name", "location").alias("dept_name_location_key"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") <= 141000), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "location", "dept_name_upper")
)

kept_keys = (
    ranked
    .select("id")
    .subtract(
        df_departments
        .filter(~F.col("location").isin(["Berlin", "London", "Bangkok"]))
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id") == 23), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("location") == "Tokyo")
    .select("*", F.upper("location").alias("location_upper"))
)

totals = (
    filtered
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
    )
)

result = (
    totals
    .select(
        "location",
        F.when(F.col("location").rlike("^(New York|Chiang Mai)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Singapore)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy(F.col("location").desc())
)
