from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "brand", "cost", "price")
    .unpivot(
        ids=["id", "brand"],
        values=["cost", "price"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .groupBy(F.when(F.col("val") >= 73, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.sort_array(F.collect_set("kpi")).alias("kpi_list"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.sort_array(F.collect_set("kpi")).alias("kpi_list_2"),
    )
)
