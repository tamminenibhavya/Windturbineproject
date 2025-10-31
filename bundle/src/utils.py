from pyspark.sql import functions as f
from pyspark.sql.types import NumericType
from pyspark.sql.types import *


def cleanse_data(df):
        """
        Cleans the data by applying below preprocessing steps:
        
        Steps performed:
        ----------------
        1. Remove duplicate rows.
        2. Identify numeric and categorical columns automatically.
        3. Handle missing values:
        - Impute numeric columns with mean.
        - Fill categorical columns with 'Unknown'.
        4. Handle outliers in numeric columns using IQR capping (Winsorization).
        5. Standardize text columns (trim spaces and lowercase).
        6. Filter rows where Integer type fields are not null.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Input raw DataFrame to clean.

        Returns
        -------
        pyspark.sql.DataFrame
            Cleaned DataFrame.
        """

        # Remove duplicate records
        df = df.dropDuplicates()

        # Identify column types (numeric vs categorical)
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
        categorical_cols = [col for col in df.columns if col not in numeric_cols]

        #  Handle missing values
        # Impute missing numeric values using column mean
        for col in numeric_cols:
            mean_value = df.select(f.mean(f.col(col))).first()[0]
            # Check if the mean_value is None
            if mean_value is not None:
                df = df.fillna({col: mean_value})
            else:
                print(f"Warning: Mean value for column '{col}' is None. Skipping imputation for this column.")
        
        # Fill missing categorical values with "Unknown"
        if categorical_cols:
            df = df.fillna("Unknown", subset=categorical_cols)

        # Handle outliers using IQR method (cap at lower/upper bounds)
        for col in numeric_cols:
            try:
                # Compute Q1 (25th percentile) and Q3 (75th percentile)
                Q1 = df.approxQuantile(col, [0.25], 0.05)[0]
                Q3 = df.approxQuantile(col, [0.75], 0.05)[0]
                IQR = Q3 - Q1

                # Define lower and upper limits
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR

                # Replace values outside bounds with limits
                df = df.withColumn(
                    col,
                    f.when(f.col(col) < lower, lower)
                    .when(f.col(col) > upper, upper)
                    .otherwise(f.col(col))
                )

            except Exception as e:
                # Skip columns that cause issues (e.g., all nulls, constants)
                continue
        
        #  Standardize categorical/text columns
        for col in categorical_cols:
            df = df.withColumn(col, f.trim(f.lower(f.col(col))))

        # Filter rows where Integer type fields are not null
        integer_cols = [f.name for f in df.schema.fields if str(f.dataType) == "IntegerType"]
        if integer_cols:
            not_null_condition = [f.col(col).isNotNull() for col in integer_cols]
            df = df.filter(f.reduce(lambda x, y: x & y, not_null_condition))

        return df
    

def calculate_turbine_summary(df, time_col="timestamp", turbine_col="turbine_id", power_col="power_output"):
    """
    Calculates daily summary statistics (min, max, avg) for each turbine.
    """

    # Extract the date part (assumes timestamp column exists)
    df = df.withColumn("date", f.to_date(f.col(time_col)))

    # Compute min, max, avg power per turbine per day
    summary_df = (
        df.groupBy(turbine_col, "date")
          .agg(
              f.min(power_col).alias("min_power"),
              f.max(power_col).alias("max_power"),
              f.avg(power_col).alias("avg_power")
          )
    )

    return summary_df

def identify_anomalies(summary_df, avg_col="avg_power", turbine_col="turbine_id", threshold=2):
    """
    Identifies turbines whose average power output deviates more than 'threshold' standard deviations
    from the mean.
    """
    stats = summary_df.select(
        f.mean(f.col(avg_col)).alias("mean_power"),
        f.stddev(f.col(avg_col)).alias("std_power")
    ).first()

    mean_val = stats["mean_power"]
    std_val = stats["std_power"]

    if std_val is None:
        # Handle missing std, e.g., skip or set limits to None
        lower_limit = None
        upper_limit = None
    else:
        lower_limit = mean_val - threshold * std_val
        upper_limit = mean_val + threshold * std_val


    anomaly_df = (
        summary_df.withColumn(
            "is_anomaly",
            (f.col(avg_col) < lower_limit) | (f.col(avg_col) > upper_limit)
        )
        .withColumn(
            "deviation",
            f.when(f.col(avg_col) < lower_limit, f.col(avg_col) - lower_limit)
             .when(f.col(avg_col) > upper_limit, f.col(avg_col) - upper_limit)
             .otherwise(f.lit(0))
        )
    )

    return anomaly_df

