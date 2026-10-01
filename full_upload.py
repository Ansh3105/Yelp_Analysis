import os
import glob
import gzip
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(r"D:\Yelp_Analysis\.env")

def compress_single_file(src_path):
    gz_path = src_path + ".gz"
    if os.path.exists(gz_path):
        return gz_path
    
    t0 = time.time()
    with open(src_path, "rb") as f_in:
        with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    orig_mb = os.path.getsize(src_path) / (1024 * 1024)
    gz_mb = os.path.getsize(gz_path) / (1024 * 1024)
    print(f"Compressed {os.path.basename(src_path)}: {orig_mb:.1f} MB -> {gz_mb:.1f} MB in {time.time()-t0:.1f}s")
    return gz_path

def main():
    print("=" * 70)
    print(" Yelp Datasets: Full Upload to Snowflake Internal Stage")
    print("=" * 70)

    # 1. Pre-compress review files locally using multi-core processing
    split_files = sorted(glob.glob(r"D:\Yelp_Analysis\split_reviews\yelp_reviews_part_*.json"))
    business_file = r"D:\Yelp_Analysis\Raw_data\yelp_academic_dataset_business.json"

    print("\nPhase 1: Pre-compressing JSON files to .json.gz locally...")
    all_files_to_compress = split_files + ([business_file] if os.path.exists(business_file) else [])
    
    with ProcessPoolExecutor() as executor:
        list(executor.map(compress_single_file, all_files_to_compress))
    
    print("\nAll files pre-compressed successfully!")

    # 2. Connect to Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse="YELP_WH",
        database="YELP_DB",
        schema="RAW",
    )
    cur = conn.cursor()

    # Ensure Stage exists
    cur.execute("CREATE STAGE IF NOT EXISTS YELP_STAGE DIRECTORY = (ENABLE = TRUE)")

    # Check already staged files
    cur.execute("LIST @YELP_STAGE")
    already_staged = {row[0].split("/")[-1]: row[1] for row in cur.fetchall()}
    print(f"\nAlready staged in Snowflake: {list(already_staged.keys())}")

    # 3. Upload review files
    gz_reviews = sorted(glob.glob(r"D:\Yelp_Analysis\split_reviews\*.json.gz"))
    print(f"\nPhase 2: Uploading {len(gz_reviews)} compressed review files to @YELP_STAGE/reviews/ ...")
    
    for idx, fpath in enumerate(gz_reviews, start=1):
        fname = os.path.basename(fpath)
        fsize_mb = os.path.getsize(fpath) / (1024 * 1024)

        if fname in already_staged:
            print(f"[{idx}/{len(gz_reviews)}] {fname} already staged ({already_staged[fname]/(1024*1024):.1f} MB) - SKIPPING.")
            continue

        print(f"[{idx}/{len(gz_reviews)}] Uploading {fname} ({fsize_mb:.1f} MB)...", flush=True)
        t0 = time.time()
        norm_path = fpath.replace("\\", "/")
        cur.execute(f"PUT file://{norm_path} @YELP_STAGE/reviews/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        print(f"    Done {fname} in {time.time()-t0:.1f}s ({fsize_mb / (time.time()-t0):.2f} MB/s)")

    # 4. Upload business file
    gz_biz = business_file + ".gz"
    if os.path.exists(gz_biz):
        fname = os.path.basename(gz_biz)
        if fname not in already_staged:
            bsize_mb = os.path.getsize(gz_biz) / (1024 * 1024)
            print(f"\nUploading business file {fname} ({bsize_mb:.1f} MB)...", flush=True)
            t0 = time.time()
            norm_bpath = gz_biz.replace("\\", "/")
            cur.execute(f"PUT file://{norm_bpath} @YELP_STAGE/business/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            print(f"    Done {fname} in {time.time()-t0:.1f}s")
        else:
            print(f"\nBusiness file {fname} already staged - SKIPPING.")

    # 5. Final Stage Listing
    print("\n" + "=" * 70)
    print(" Final Stage Listing (@YELP_STAGE)")
    print("=" * 70)
    cur.execute("LIST @YELP_STAGE")
    total_size = 0
    for row in cur.fetchall():
        total_size += row[1]
        print(f"  {row[0]:<55} | {row[1]/(1024*1024):.2f} MB")
    print(f"\nTotal Data Staged: {total_size/(1024*1024):.2f} MB")
    print("Upload complete!")

    conn.close()

if __name__ == "__main__":
    main()
