from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host", "level").orderBy("flag_bit2")
w2 = Window.partitionBy("host")
w_hist = Window.orderBy("status_code_high")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("host") != "h1")
    .select(
        "id",
        "host",
        "level",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

with_windows = (
    parsed
    .filter(F.col("host") == "h3")
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("flag_bit2_dense_rank"),
        F.count("flag_bit2").over(w2).alias("host_count_flag_bit2"),
        F.cume_dist().over(w_hist).alias("status_code_high_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.round((F.col("host_count_flag_bit2") / F.sum("host_count_flag_bit2").over(w2)), 4).alias("host_count_flag_bit2_share"),
    )
)

cleaned = (
    enriched
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "host",
        "level",
        "value",
        "status_code_high",
        "flag_bit2",
        "flag_bit2_dense_rank",
        "host_count_flag_bit2",
        "status_code_high_cume_dist",
        "host_count_flag_bit2_share",
        F.upper("host").alias("host_upper"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "level",
        "value",
        "status_code_high",
        "flag_bit2",
        "flag_bit2_dense_rank",
        "host_count_flag_bit2",
        "status_code_high_cume_dist",
        "host_count_flag_bit2_share",
    )
    .orderBy("flag_bit2", F.col("status_code_high_cume_dist").desc())
)
