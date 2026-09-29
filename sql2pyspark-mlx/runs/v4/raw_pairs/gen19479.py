from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("num_context").desc_nulls_last())

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        F.size("context").alias("num_context"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.count("num_context").over(w).alias("service_count_num_context"),
        F.cume_dist().over(w2).alias("num_context_cume_dist"),
    )
)
