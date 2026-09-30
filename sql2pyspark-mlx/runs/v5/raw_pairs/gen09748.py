from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("kpi").orderBy(F.col("distinct_ids").desc())

metrics_long = (
    df_products
    .select("id", "category", "cost", "price")
    .unpivot(
        ids=["id", "category"],
        values=["cost", "price"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)

result = (
    metrics_long
    .groupBy(
        "kpi",
        F.when(F.col("reading") >= 71, "LARGE").otherwise("SMALL").alias("reading_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.sort_array(F.collect_set("kpi")).alias("kpi_list"),
        F.sum(F.when(F.col("kpi") == "cost", F.col("reading")).otherwise(0)).alias("cost_reading"),
        F.count_if(F.col("kpi") == "cost").alias("cost_events"),
        F.count("*").alias("num_rows"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w))
    .orderBy(F.col("num_rows").desc_nulls_last())
)
