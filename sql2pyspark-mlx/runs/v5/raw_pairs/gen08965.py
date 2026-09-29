from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        F.size("context").alias("num_context"),
        F.col("context").getItem("version").alias("version"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .withColumn("flag_bit2_tier", F.when(F.col("flag_bit2") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["flag_bit2_tier", "service"], ["flag_bit2_tier"]],
        "flag_bit2_tier",
        "service",
    )
    .agg(
        F.round(F.sum("num_context") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("num_context_per_auth"),
        F.sum(F.when(F.col("service") == "auth", F.col("num_context")).otherwise(0)).alias("auth_num_context"),
        F.min("flag_bit2").alias("min_flag_bit2"),
    )
    .orderBy("service")
)
