# multicore_features_opencv.py
# Parallel version of imag_features_opencv.py using multiprocessing.

import json
import os
import time
import subprocess
import numpy as np
import cv2
from multiprocessing import Pool, cpu_count
from config import IMAGE_SIZE

def compute_hog_like_features(gray: np.ndarray, num_bins: int = 9) -> np.ndarray:
    gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    mag, angle = cv2.cartToPolar(gx, gy, angleInDegrees=True)
    angle = np.mod(angle, 180.0)
    bin_edges = np.linspace(0.0, 180.0, num_bins + 1)
    hog = np.zeros(num_bins, dtype=np.float32)
    for i in range(num_bins):
        mask = (angle >= bin_edges[i]) & (angle < bin_edges[i + 1])
        hog[i] = float(mag[mask].sum())
    norm = np.linalg.norm(hog) + 1e-6
    hog /= norm
    return hog

def compute_visual_features(image_array) -> dict:
    img = np.array(image_array, dtype=np.uint8)
    if img.ndim == 2:
        img = np.stack([img] * 3, axis=-1)
    elif img.ndim == 3 and img.shape[2] == 4:
        img = img[:, :, :3]
    if tuple(img.shape[:2]) != tuple(IMAGE_SIZE):
        img = cv2.resize(img, IMAGE_SIZE)
    r, g, b = img[:, :, 0], img[:, :, 1], img[:, :, 2]
    r_mean, r_std = float(r.mean()), float(r.std())
    g_mean, g_std = float(g.mean()), float(g.std())
    b_mean, b_std = float(b.mean()), float(b.std())
    gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    gray_mean, gray_std = float(gray.mean()), float(gray.std())
    lap = cv2.Laplacian(gray, cv2.CV_64F)
    lap_var = float(lap.var())
    edges = cv2.Canny(gray, 100, 200)
    edge_density = float(np.count_nonzero(edges)) / float(edges.size)
    hog_vec = compute_hog_like_features(gray, num_bins=9)
    hog_features = {f"hog_{i}": float(hog_vec[i]) for i in range(len(hog_vec))}
    features = {
        "r_mean": r_mean, "g_mean": g_mean, "b_mean": b_mean,
        "r_std": r_std, "g_std": g_std, "b_std": b_std,
        "gray_mean": gray_mean, "gray_std": gray_std,
        "laplacian_var": lap_var, "edge_density": edge_density
    }
    features.update(hog_features)
    return features

def process_record(record):
    if "image_array" not in record:
        return None
    try:
        feats = compute_visual_features(record["image_array"])
        new_record = dict(record)
        new_record.update(feats)
        return new_record
    except Exception as e:
        print(f"Error processing image_id={record.get('image_id')}: {e}")
        return None

def main():
    input_json = "image_metadata.json"
    output_json = "image_metadata_with_features_multicore.json"
    hdfs_path = "/user/hadoop/image_analysis/image_metadata_with_features.json"

    if not os.path.exists(input_json):
        print(f"Input not found: {input_json}")
        return

    with open(input_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total records: {len(data)}")

    start_time = time.time()

    num_processes = cpu_count()
    with Pool(processes=num_processes) as pool:
        enriched = pool.map(process_record, data)

    enriched = [rec for rec in enriched if rec is not None]

    end_time = time.time()
    print(f"Multicore feature extraction done in {end_time - start_time:.2f} seconds.")
    print(f"Successful: {len(enriched)}, Failed: {len(data) - len(enriched)}")

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    # Upload to HDFS (optional)
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/hadoop/image_analysis"], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", output_json, hdfs_path], check=True)
        print("Uploaded to HDFS.")
    except Exception as e:
        print(f"HDFS upload failed: {e}")

if __name__ == "__main__":
    main()