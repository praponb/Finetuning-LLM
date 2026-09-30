from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    long_format
    .groupBy("metric_name")
    .agg(
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("metric_name") == "price", 1)), F.lit(0)), 2).alias("val_per_price"),
        F.count_if(F.col("metric_name") == "cost").alias("cost_events"),
        F.count_if(F.col("metric_name") == "price").alias("price_events"),
        F.sum(F.when(F.col("metric_name") == "price", F.col("val")).otherwise(0)).alias("price_val"),
        F.max(F.when(F.col("metric_name") == "cost", F.col("val"))).alias("cost_max_val"),
    )
    .orderBy(F.col("cost_events").desc())
)
