from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_type") != "click") & F.col("page").isin(["/search", "/checkout", "/product"]))
)

agg = (
    base
    .groupBy(
        F.when(F.col("page").isin(["/cart", "/product"]), "priority").otherwise("other").alias("page_group"),
    )
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = (
    agg
    .select("page_group", "event_type_list")
    .orderBy("page_group")
)
