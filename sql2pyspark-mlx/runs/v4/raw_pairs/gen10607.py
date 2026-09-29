from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@GMAIL.COM"))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "is_remote")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)
