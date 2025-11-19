# image_features_opencv.py
# Extract visual features from scraped images using OpenCV.
# Input:  image_metadata.json  (created by parallel_scraper_multiprocessing.py)
# Output: image_metadata_with_features.json (+ optional upload to HDFS)

import json
import os
import time
import subprocess

import numpy as np
import cv2

from config import IMAGE_SIZE  # (64, 64) from your config


def compute_hog_like_features(gray: np.ndarray, num_bins: int = 9) -> np.ndarray:
    """
    Simple HOG-like descriptor:
    - Compute gradient (Sobel)
    - Convert to magnitude + orientation
    - Bin orientations into num_bins between 0 and 180 degrees
    - L2-normalise the histogram
    """
    gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)

    mag, angle = cv2.cartToPolar(gx, gy, angleInDegrees=True)
    # Restrict orientation to [0, 180)
    angle = np.mod(angle, 180.0)

    bin_edges = np.linspace(0.0, 180.0, num_bins + 1)
    hog = np.zeros(num_bins, dtype=np.float32)

    for i in range(num_bins):
        mask = (angle >= bin_edges[i]) & (angle < bin_edges[i + 1])
        # Sum magnitudes in this orientation bin
        hog[i] = float(mag[mask].sum())

    # L2 normalisation to make features scale-invariant
    norm = np.linalg.norm(hog) + 1e-6
    hog /= norm
    return hog


def compute_visual_features(image_array) -> dict:
    """
    image_array: nested Python list or numpy array of shape (H, W, 3)
    Returns a dict of scalar visual features ready to be merged into JSON.
    """
    img = np.array(image_array, dtype=np.uint8)

    # Ensure we have 3 channels
    if img.ndim == 2:
        img = np.stack([img] * 3, axis=-1)
    elif img.ndim == 3 and img.shape[2] == 4:
        # Drop alpha channel if present
        img = img[:, :, :3]

    # Resize to IMAGE_SIZE if necessary
    if tuple(img.shape[:2]) != tuple(IMAGE_SIZE):
        img = cv2.resize(img, IMAGE_SIZE)

    # Treat array as RGB (this is how PIL gave it to you)
    r = img[:, :, 0]
    g = img[:, :, 1]
    b = img[:, :, 2]

    r_mean, r_std = float(r.mean()), float(r.std())
    g_mean, g_std = float(g.mean()), float(g.std())
    b_mean, b_std = float(b.mean()), float(b.std())

    # Grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    gray_mean = float(gray.mean())
    gray_std = float(gray.std())

    # Laplacian variance – sharpness / texture
    lap = cv2.Laplacian(gray, cv2.CV_64F)
    lap_var = float(lap.var())

    # Edge density – fraction of edge pixels
    edges = cv2.Canny(gray, 100, 200)
    edge_density = float(np.count_nonzero(edges)) / float(edges.size)

    # HOG-like descriptor (9 bins)
    hog_vec = compute_hog_like_features(gray, num_bins=9)
    hog_features = {f"hog_{i}": float(hog_vec[i]) for i in range(len(hog_vec))}

    features = {
        "r_mean": r_mean,
        "g_mean": g_mean,
        "b_mean": b_mean,
        "r_std": r_std,
        "g_std": g_std,
        "b_std": b_std,
        "gray_mean": gray_mean,
        "gray_std": gray_std,
        "laplacian_var": lap_var,
        "edge_density": edge_density,
    }
    features.update(hog_features)
    return features


def main():
    input_json = "image_metadata.json"
    output_json = "image_metadata_with_features.json"
    hdfs_path = "/user/hadoop/image_analysis/image_metadata_with_features.json"

    if not os.path.exists(input_json):
        print(f"❌ Input file not found: {input_json}")
        return

    print(f"📥 Loading scraped metadata from {input_json} ...")
    with open(input_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"🔢 Total records: {len(data)}")

    start_time = time.time()
    enriched = []
    num_ok, num_failed = 0, 0

    for i, record in enumerate(data):
        if "image_array" not in record:
            print(f"⚠️  Record {i} (image_id={record.get('image_id')}) has no 'image_array'. Skipped.")
            num_failed += 1
            continue

        try:
            feats = compute_visual_features(record["image_array"])
            new_record = dict(record)
            new_record.update(feats)
            enriched.append(new_record)
            num_ok += 1

            if (i + 1) % 10 == 0:
                print(f"   ✅ Processed {i + 1} / {len(data)} images...")
        except Exception as e:
            print(f"❌ Feature extraction failed for image_id={record.get('image_id')}: {e}")
            num_failed += 1

    end_time = time.time()
    print("-----------------------------------------")
    print(f"✅ Feature extraction done in {end_time - start_time:.2f} seconds.")
    print(f"   ✅ Successful: {num_ok}")
    print(f"   ❌ Failed:     {num_failed}")
    print("-----------------------------------------")

    # Save enriched JSON locally
    print(f"💾 Saving enriched metadata with features to {output_json} ...")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    print("   ✅ Saved.")

    # Optional: upload to HDFS so Spark/Scala can use it directly
    print(f"🌍 Uploading {output_json} to HDFS at {hdfs_path} ...")
    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/image_analysis"],
            check=False
        )
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", output_json, hdfs_path],
            check=True
        )
        print("   ✅ Successfully uploaded to HDFS.")
    except Exception as e:
        print(f"   ❌ HDFS upload failed: {e}")

    print("🎉 image_features_opencv.py finished.")


if __name__ == "__main__":
    main()
