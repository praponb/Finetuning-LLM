from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)
