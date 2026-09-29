from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("budget").between(2307000, 4020000))
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

result = (
    cleaned
    .groupBy("location")
    .pivot("dept_name", ["Sales", "Finance", "Legal"])
    .agg(F.max("budget_int").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "location",
        F.col("Sales_total").alias("sales_total"),
        F.col("Sales_cnt").alias("sales_cnt"),
        F.col("Finance_total").alias("finance_total"),
        F.col("Finance_cnt").alias("finance_cnt"),
        F.col("Legal_total").alias("legal_total"),
        F.col("Legal_cnt").alias("legal_cnt"),
    )
)
