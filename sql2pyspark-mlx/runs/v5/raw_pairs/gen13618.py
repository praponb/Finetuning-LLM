from pyspark.sql import functions as F

result = (
    df_sessions
    .groupingSets([["device", "country"], ["device"], []], "device", "country")
    .agg(
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.grouping_id().alias("gid"),
    )
)
