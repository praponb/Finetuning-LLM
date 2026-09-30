from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "category", "price", "cost")
    .unpivot(
        ids=["id", "category"],
        values=["price", "cost"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("reading") >= 87, "PREMIUM").otherwise("STANDARD").alias("reading_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("kpi")).alias("kpi_list"),
        F.percentile_approx("reading", [0.25, 0.5, 0.75]).alias("reading_quartiles"),
        F.sum("reading").alias("total_reading"),
        F.max(F.when(F.col("kpi") == "cost", F.col("reading"))).alias("cost_max_reading"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
    )
)
