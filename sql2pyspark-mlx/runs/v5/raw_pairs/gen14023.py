from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("service").orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .filter(F.col("attr_key") != "trace")
    .select(
        "id",
        "service",
        F.rank().over(w_grp).alias("status_code_rank"),
        F.rank().over(w2).alias("status_code_rank_2"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
    )
)
