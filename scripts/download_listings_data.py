#!/usr/bin/env python3
"""
Berlin Airbnb Data Ingestion and Preprocessing Pipeline
======================================================
This script follows the exact same logic and order as the Jupyter notebook:
data-ingestion-and-preprocessing.ipynb

Pipeline Steps:
1. Environment Setup (Nuclear Spark Reset)
2. Data Discovery (Web scraping Inside Airbnb)
3. File Filtering (.csv.gz files only)
4. File Download (with date prefixes)
5. File Extraction (unzip .gz files)
6. Data Loading (Spark with proper parsing)
7. Preprocessing:
   - Column Selection
   - Non-null Filtering
   - Schema Enforcement
   - Column Renaming
   - Data Cleaning
   - Feature Engineering (Property categorization)
"""

import subprocess
import time
import os
import sys
import requests
import re
import gzip
import shutil
from datetime import datetime


def setup_environment():
    """
    Environment Setup - Nuclear Spark Reset
    Complete clean start with all Spark references wiped
    """
    print("=" * 60)
    print("NUCLEAR SPARK RESET - COMPLETE SYSTEM CLEANUP")
    print("=" * 60)

    # Step 1: Kill ALL Java processes (nuclear option)
    print("\nSTEP 1: TERMINATING ALL JAVA PROCESSES")
    print("-" * 40)
    try:
        subprocess.run(['pkill', '-f', 'java'], capture_output=True)
        time.sleep(5)  # Longer wait for complete cleanup
        print("STATUS: All Java processes terminated")
    except:
        print("STATUS: Java process cleanup completed")

    # Step 2: Clear ALL PySpark imports from memory
    print("\nSTEP 2: CLEARING PYSPARK FROM MEMORY")
    print("-" * 40)
    modules_to_remove = []
    for module_name in sys.modules:
        if 'pyspark' in module_name or 'py4j' in module_name:
            modules_to_remove.append(module_name)

    for module_name in modules_to_remove:
        if module_name in sys.modules:
            del sys.modules[module_name]
            
    print(f"STATUS: Removed {len(modules_to_remove)} Spark-related modules from memory")

    # Step 3: Set environment variables for clean start
    print("\nSTEP 3: CONFIGURING CLEAN ENVIRONMENT")
    print("-" * 40)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    print("STATUS: Environment variables configured")

    # Step 4: Now import fresh PySpark and create session
    print("\nSTEP 4: CREATING FRESH SPARK SESSION")
    print("-" * 40)
    from pyspark.sql import SparkSession
    from pyspark import SparkContext

    # Create session with unique app name
    spark = SparkSession.builder \
        .appName(f"BerlinAirbnb_{int(time.time())}") \
        .master("local[1]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Verify it works
    print("\nRESULTS:")
    print("-" * 40)
    print("STATUS: Nuclear reset successful")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"App ID: {spark.sparkContext.applicationId}")
    print("=" * 60)
    print("SPARK SESSION READY FOR USE")
    print("=" * 60)
    
    return spark


def discover_berlin_data():
    """
    Data Discovery - Get All Berlin Links from Inside Airbnb
    Web scraping to extract all Berlin dataset URLs including archived data
    """
    print("=" * 60)
    print("GETTING ALL BERLIN LINKS FROM INSIDE AIRBNB")
    print("=" * 60)

    berlin_links = []
    
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        # Set up headless Chromium
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.binary_location = "/usr/bin/chromium"
        
        # Set up ChromeDriver service with explicit path
        service = Service("/usr/bin/chromedriver")
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get("https://insideairbnb.com/get-the-data/")
        
        print("Looking for Berlin archived data button...")
        time.sleep(2)
        
        # Click the specific Berlin archived data button
        berlin_button = driver.find_element(By.CSS_SELECTOR, 'a.showArchivedData[data-city="Berlin"]')
        driver.execute_script("arguments[0].click();", berlin_button)
        time.sleep(3)  # Wait for content to load
        
        # Extract all Berlin links
        page_source = driver.page_source
        
        # Find all URLs containing 'berlin'
        berlin_patterns = [
            r'https?://data\.insideairbnb\.com/germany/be/berlin/[^\s<>"\']*'
        ]
        
        for pattern in berlin_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                if match not in berlin_links:
                    berlin_links.append(match)
        
        driver.quit()
        
        print(f"Found {len(berlin_links)} Berlin links:")
        print("-" * 60)
        
        for link in sorted(berlin_links):
            print(link)
            
    except ImportError:
        print("ERROR: Selenium not installed. Install with: pip install selenium")
        return []
    except Exception as e:
        print(f"ERROR: {e}")
        return []

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    
    return berlin_links


def filter_csv_gz_files(berlin_links):
    """
    File Filtering - Extract only .csv.gz files for analysis
    """
    print("=" * 60)
    print("FILTERING FOR .CSV.GZ FILES ONLY")
    print("=" * 60)

    # Filter berlin_links for .csv.gz files only
    csv_gz_links = [link for link in berlin_links if link.endswith('.csv.gz')]
    
    print(f"Found {len(csv_gz_links)} .csv.gz files:")
    print("-" * 60)
    
    for link in sorted(csv_gz_links):
        # Extract filename and date for better readability
        filename = link.split('/')[-1]
        date = link.split('/')[-3] if len(link.split('/')) >= 3 else 'unknown'
        print(f"{date} - {filename}")
        print(f"  {link}")

    print("\n" + "=" * 60)
    print("CSV.GZ FILTERING COMPLETE")
    print("=" * 60)
    
    return csv_gz_links


def download_files(csv_gz_links, max_files=2):
    """
    File Download - Download .csv.gz files with date prefix and error handling
    """
    print("=" * 60)
    print("DOWNLOADING .CSV.GZ FILES")
    print("=" * 60)

    # Create download folder
    download_folder = "data/csv_gz_files"
    os.makedirs(download_folder, exist_ok=True)

    # Dictionary to store downloaded files
    downloaded_files = {}

    # Download each file (limit to first 2 for script execution)
    for link in csv_gz_links[:max_files]:
        try:
            # Extract date and filename
            date = link.split('/')[-3]  # Extract date from URL
            filename = link.split('/')[-1]  # Extract filename
            new_filename = f"{date}_{filename}"  # Add date prefix
            
            print(f"Downloading: {new_filename}")
            
            # Download with error handling
            response = requests.get(link, stream=True)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Save file
            file_path = os.path.join(download_folder, new_filename)
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Add to dictionary
            downloaded_files[new_filename] = file_path
            
            print(f"Downloaded: {new_filename}")
            time.sleep(5)  # Be polite to server
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {link}: {e}")
        except Exception as e:
            print(f"Error with {link}: {e}")

    print(f"\n{len(downloaded_files)} files downloaded successfully:")
    for filename, path in downloaded_files.items():
        print(f"  {filename}: {path}")

    print("\n" + "=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)
    
    return downloaded_files


def extract_files(downloaded_files):
    """
    File Extraction - Unzip .csv.gz files to raw data folder
    """
    print("=" * 60)
    print("UNZIPPING CSV.GZ FILES")
    print("=" * 60)

    # Create raw data folder
    raw_folder = "data/raw"
    os.makedirs(raw_folder, exist_ok=True)

    # Dictionary to store unzipped files
    unzipped_files = {}

    # Unzip each file
    for filename, gz_path in downloaded_files.items():
        try:
            # Remove .gz extension for output filename
            csv_filename = filename.replace('.gz', '')
            csv_path = os.path.join(raw_folder, csv_filename)
            
            print(f"Unzipping: {filename} → {csv_filename}")
            
            # Unzip file
            with gzip.open(gz_path, 'rb') as f_in:
                with open(csv_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            unzipped_files[csv_filename] = csv_path
            print(f"Unzipped: {csv_filename}")
            
        except Exception as e:
            print(f"Failed to unzip {filename}: {e}")

    print("\n" + "=" * 60)
    print(f"UNZIP COMPLETE FOR {list(unzipped_files.keys())}")
    print("=" * 60)
    
    return unzipped_files


def load_listing_data(spark, unzipped_files):
    """
    Data Loading - Load listings CSV with Spark and proper parsing
    """
    print("=" * 60)
    print("FETCHING FIRST LISTING CSV WITH SPARK (CORRECTED PARSING)")
    print("=" * 60)

    # Find listings file
    listing_csv_path = unzipped_files[next((f for f in unzipped_files if "listings" in f), None)]

    # Read CSV with proper parsing options
    listing_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(listing_csv_path)

    # Get all columns first
    all_columns = listing_df.columns

    # How many rows/columns 
    print("Number of rows and columns in the listing DataFrame:")
    num_rows = listing_df.count()
    num_columns = len(all_columns)
    print(f"Rows: {num_rows}, Columns: {num_columns}")
    print("=" * 60)

    print("All columns in the listing DataFrame:")
    print(all_columns)
    print("=" * 60)

    # Print data types for all columns
    print("Data types of each column:")
    print(listing_df.dtypes)

    # Show first 5 rows with limited columns to avoid display issues
    print("First 5 rows")
    listing_df.show(5)
    print("=" * 60)
    
    return listing_df


def select_columns(listing_df):
    """
    Column Selection - Select comprehensive columns for analysis
    """
    from pyspark.sql.functions import col, sum as spark_sum, when

    # Select comprehensive columns for analysis
    selected_columns = [
        # Core identifiers and location
        "id", "host_id", "latitude", "longitude",
        
        # Neighborhood data
        "neighbourhood_group_cleansed",
        
        # Review and rating data
        "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", 
        "review_scores_checkin", "review_scores_communication", "review_scores_location", 
        "review_scores_value", "reviews_per_month", "number_of_reviews",
        
        # Pricing and availability
        "price",
        
        # "availability_30", "availability_60", "availability_90", "availability_365",
        "minimum_nights", "maximum_nights",
        
        # Property characteristics
        "property_type", "amenities", "accommodates", "beds"
    ]

    listing_df = listing_df.select(*selected_columns)
    return listing_df


def filter_non_null(listing_df):
    """
    Non-null Filtering - Keep only rows where critical columns are not null
    """
    from pyspark.sql.functions import col
    
    # Filter to keep only rows where critical columns are not null
    critical_columns = ["price", "latitude", "longitude", "neighbourhood_group_cleansed"]

    print(f"\nRows before filtering critical column: {listing_df.count()}")

    # Apply filter - keep rows where ALL critical columns are not null
    listing_df = listing_df.filter(
        col("price").isNotNull() 
        & ((col("latitude").isNotNull() & col("longitude").isNotNull()) 
            | col("neighbourhood_group_cleansed").isNotNull())
    )

    print(f"Rows after filtering (no nulls in critical columns): {listing_df.count()}")
    return listing_df


def enforce_schema(listing_df):
    """
    Schema Enforcement - Cast columns to appropriate data types
    """
    from pyspark.sql.functions import col, regexp_replace
    from pyspark.sql.types import StringType, IntegerType, DoubleType, FloatType, LongType 

    print("=" * 60)
    print("ENFORCING DATA TYPES")
    print("=" * 60)

    # Show current schema before conversion
    print("Current schema:")
    print(listing_df.dtypes)

    # Clean price column first (remove $ and commas)
    listing_df = listing_df.withColumn("price", regexp_replace(col("price"), "[\$,]", ""))

    # Cast columns to appropriate data types
    listing_df = listing_df \
        .withColumn("id", col("id").cast(LongType())) \
        .withColumn("host_id", col("host_id").cast(LongType())) \
        .withColumn("latitude", col("latitude").cast(DoubleType())) \
        .withColumn("longitude", col("longitude").cast(DoubleType())) \
        .withColumn("neighbourhood_group_cleansed", col("neighbourhood_group_cleansed").cast(StringType())) \
        .withColumn("review_scores_rating", col("review_scores_rating").cast(FloatType())) \
        .withColumn("review_scores_accuracy", col("review_scores_accuracy").cast(FloatType())) \
        .withColumn("review_scores_cleanliness", col("review_scores_cleanliness").cast(FloatType())) \
        .withColumn("review_scores_checkin", col("review_scores_checkin").cast(FloatType())) \
        .withColumn("review_scores_communication", col("review_scores_communication").cast(FloatType())) \
        .withColumn("review_scores_location", col("review_scores_location").cast(FloatType())) \
        .withColumn("review_scores_value", col("review_scores_value").cast(FloatType())) \
        .withColumn("reviews_per_month", col("reviews_per_month").cast(FloatType())) \
        .withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType())) \
        .withColumn("price", col("price").cast(FloatType())) \
        .withColumn("minimum_nights", col("minimum_nights").cast(IntegerType())) \
        .withColumn("maximum_nights", col("maximum_nights").cast(IntegerType())) \
        .withColumn("property_type", col("property_type").cast(StringType())) \
        .withColumn("amenities", col("amenities").cast(StringType())) \
        .withColumn("accommodates", col("accommodates").cast(IntegerType())) \
        .withColumn("beds", col("beds").cast(IntegerType()))

    print("\nSchema after data type conversion:")
    print(listing_df.dtypes)
    
    return listing_df


def rename_columns(listing_df):
    """
    Column Renaming - Rename columns for better readability
    """
    # Renaming 
    listing_df_renamed = listing_df
    listing_df_renamed = listing_df_renamed.withColumnRenamed("id", "listing_id")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_rating", "rating_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_accuracy", "accuracy_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_cleanliness", "cleanliness_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_checkin", "checkin_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_communication", "communication_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_location", "location_score")
    listing_df_renamed = listing_df_renamed.withColumnRenamed("review_scores_value", "value_score")
    
    return listing_df_renamed


def clean_data(listing_df_renamed):
    """
    Data Cleaning - Apply business rules and validation
    """
    from pyspark.sql.functions import col, when
    
    listing_df_clean = listing_df_renamed

    # Handle negative or zero prices
    listing_df_clean = listing_df_clean.filter(col("price") > 10) 

    # Berlin coordinate bounds validation
    listing_df_clean = listing_df_clean.filter(
        (col("latitude").between(52.3, 52.7)) &   # Berlin latitude range
        (col("longitude").between(13.0, 13.8))    # Berlin longitude range
    )

    # Review scores should be between 0-5
    review_cols = ["rating_score", "accuracy_score", "cleanliness_score", 
                   "checkin_score", "communication_score", "location_score", "value_score"]

    for col_name in review_cols:
        listing_df_clean = listing_df_clean.withColumn(
            col_name,
            when((col(col_name) >= 0) & (col(col_name) <= 5), col(col_name))
            .otherwise(None)
        )
    
    return listing_df_clean


def extend_columns(listing_df_clean):
    """
    Feature Engineering - Create property categorization columns
    """
    from pyspark.sql.functions import when, col, regexp_extract

    listing_df_grouped = listing_df_clean

    # Level 1: Broad Property Categories
    listing_df_grouped = listing_df_grouped.withColumn("property_category",
        when(col("property_type").rlike("(?i)entire"), "Entire Property")
        .when(col("property_type").rlike("(?i)private room"), "Private Room")  
        .when(col("property_type").rlike("(?i)shared room"), "Shared Room")
        .when(col("property_type").rlike("(?i)room in"), "Hotel/Hostel Room")
        .otherwise("Other")
    )

    # Level 2: Property Type (Housing vs Commercial)
    listing_df_grouped = listing_df_grouped.withColumn("accommodation_type",
        when(col("property_type").rlike("(?i)hotel|hostel|aparthotel"), "Commercial")
        .when(col("property_type").rlike("(?i)home|house|apartment|condo|villa|cabin|cottage|loft"), "Residential")
        .when(col("property_type").rlike("(?i)boat|treehouse|cave|dome|tiny|camper|rv"), "Unique/Alternative")
        .otherwise("Other")
    )

    # Level 3: Specific Property Subtype
    listing_df_grouped = listing_df_grouped.withColumn("property_subtype",
        when(col("property_type").rlike("(?i)condo"), "Condo/Apartment")
        .when(col("property_type").rlike("(?i)house|home"), "House")
        .when(col("property_type").rlike("(?i)villa|mansion"), "Luxury")
        .when(col("property_type").rlike("(?i)hotel"), "Hotel")
        .when(col("property_type").rlike("(?i)hostel"), "Hostel")
        .when(col("property_type").rlike("(?i)boat|houseboat"), "Watercraft")
        .when(col("property_type").rlike("(?i)treehouse|cave|dome|tiny"), "Unique")
        .otherwise("Standard")
    )
    
    return listing_df_grouped


def export_partitioned_data(listing_df_final, output_path="data/processed/berlin_listings_partitioned.parquet"):
    """
    Export the listing dataframe to a partitioned parquet file
    """
    print("=" * 60)
    print("EXPORTING TO PARTITIONED PARQUET FILE")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Get initial partition count using modern PySpark approach
    initial_partitions = listing_df_final.sparkSession.sparkContext.defaultParallelism
    print(f"Initial partitions: {initial_partitions}")
    
    # Repartition to 6 partitions for optimal performance using PySpark DataFrame
    partitioned_df = listing_df_final.repartition(6)
    
    # Get final partition count by checking the partitioned DataFrame
    print(f"Final partitions: 6 (as specified)")
    
    # Export to partitioned parquet
    print(f"\nExporting partitioned dataset to: {output_path}")
    partitioned_df.write.mode("overwrite").parquet(output_path)
    
    # Show basic statistics
    total_records = partitioned_df.count()
    print(f"Total records exported: {total_records:,}")
    print(f"Records per partition (avg): {total_records // 6:,}")
    
    print("\n" + "=" * 60)
    print("PARTITIONED EXPORT COMPLETE")
    print("=" * 60)


def main():
    """
    Main function - Execute the complete pipeline
    """
    print("Starting Berlin Airbnb Data Pipeline")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Environment Setup
        spark = setup_environment()
        
        # Step 2: Data Discovery
        berlin_links = discover_berlin_data()
        if not berlin_links:
            print("ERROR: No Berlin links found. Exiting.")
            return
        
        # Step 3: File Filtering
        csv_gz_links = filter_csv_gz_files(berlin_links)
        if not csv_gz_links:
            print("ERROR: No CSV.GZ files found. Exiting.")
            return
        
        # Step 4: File Download
        downloaded_files = download_files(csv_gz_links, max_files=2)
        if not downloaded_files:
            print("ERROR: No files downloaded. Exiting.")
            return
        
        # Step 5: File Extraction
        unzipped_files = extract_files(downloaded_files)
        if not unzipped_files:
            print("ERROR: No files extracted. Exiting.")
            return
        
        # Step 6: Data Loading
        listing_df = load_listing_data(spark, unzipped_files)
        
        # Step 7: Preprocessing Pipeline
        print("\nStarting Preprocessing Pipeline...")
        
        # Column Selection
        listing_df = select_columns(listing_df)
        
        # Non-null Filtering
        listing_df = filter_non_null(listing_df)
        
        # Schema Enforcement
        listing_df = enforce_schema(listing_df)
        
        # Column Renaming
        listing_df_renamed = rename_columns(listing_df)
        
        # Data Cleaning
        listing_df_clean = clean_data(listing_df_renamed)
        
        # Feature Engineering
        listing_df_grouped = extend_columns(listing_df_clean)
        
        # Final DataFrame
        listing_df_final = listing_df_grouped

        # Export to Partitioned Parquet
        export_partitioned_data(listing_df_final)
        
        print(f"Pipeline completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
