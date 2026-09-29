from pyspark.sql import functions as F

lines = df_app_logs.select("id", "host", F.explode("context").alias("key", "attr_value"))

result = (
    lines
    .select(
        "id",
        "host",
        "key",
        F.when(F.col("host").rlike("^(h4|h3)$"), "GROUP_B").when(F.col("host").rlike("^(h1|h2)$"), "CORE").otherwise("GROUP_A").alias("host_zone"),
    )
    .orderBy("host_zone")
)
