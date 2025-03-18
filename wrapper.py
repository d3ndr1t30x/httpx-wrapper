import subprocess
import pandas as pd
from pathlib import Path
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
DELAY_SECONDS = 2  # Delay between each domain check (adjust as needed)

# Load the CSV file
csv_file = "crm_data - Copy.csv"  # Replace with your CSV file path
logger.info(f"Loading CSV file: {csv_file}")
df = pd.read_csv(csv_file)

# Append "-tested" to the filename before the extension
input_path = Path(csv_file)
output_file = input_path.with_stem(input_path.stem + "-tested")
logger.info(f"Creating output file: {output_file}")

# Create the output file immediately and write the header
df.to_csv(output_file, index=False)
logger.info("Output file created with header.")

# Function to check if a domain is active using httpx.exe
def check_domain_status(domain):
    try:
        logger.info(f"Checking domain: {domain}")
        # Run httpx.exe with the domain as an argument
        result = subprocess.run(
            ["httpx", "-u", domain, "-silent"],  # Assuming httpx is in your PATH
            capture_output=True,
            text=True,
            timeout=10,  # Adjust timeout as needed
        )
        
        # If httpx.exe returns output, the domain is active
        if result.stdout.strip():
            logger.info(f"Domain {domain} is active")
            return "active"
        else:
            logger.info(f"Domain {domain} is dead")
            return "dead"
    except subprocess.TimeoutExpired:
        logger.warning(f"Domain {domain} timed out")
        return "dead"
    except Exception as e:
        logger.error(f"Error checking domain {domain}: {e}")
        return "dead"

# Iterate through the rows and update the status
for index, row in df.iterrows():
    website = row["website"]  # Use the "website" column for domains
    if pd.notna(website):  # Check if the website field is not empty
        status = check_domain_status(website)
        if status == "dead":
            # Update ONLY the status column
            df.at[index, "status"] = "dead"
            logger.info(f"Updated status for {website} to 'dead'")
    else:
        logger.warning(f"Skipping row {index + 1}: 'website' field is empty")

    # Append the updated row (with all columns) to the output CSV file
    df.iloc[[index]].to_csv(output_file, mode="a", header=False, index=False)
    logger.info(f"Row {index + 1} written to output file.")

    # Add a delay between requests
    logger.info(f"Waiting for {DELAY_SECONDS} seconds before the next request...")
    time.sleep(DELAY_SECONDS)

logger.info("Script completed successfully!")
