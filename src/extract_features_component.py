import os
import time
import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from tqdm import tqdm

from skimage import io, color
from skimage.filters.rank import entropy
from skimage.morphology import disk
from scipy import ndimage as nd
from skimage.filters import sobel, gabor, hessian, prewitt
from skimage.feature import graycomatrix, graycoprops


# ----------------- GLCM settings -----------------
ANGLES = [0, np.pi / 4, np.pi / 2, 3 * np.pi / 4]
DISTANCES = [1]
GLCM_PROPS = ["contrast", "dissimilarity", "homogeneity", "ASM", "energy", "correlation"]


def load_gray(path: str) -> np.ndarray:
    """
    Load an image from disk and convert to grayscale float32.
    Handles grayscale, RGB, and RGBA (4 channels).
    """
    img = io.imread(path)

    # grayscale already
    if img.ndim == 2:
        img = img.astype(np.float32)
        return img

    # color images
    if img.ndim == 3:
        # RGBA (4 channels)
        if img.shape[2] == 4:
            img = color.rgba2rgb(img)  # -> RGB float
        elif img.shape[2] > 4:
            # keep first 3 channels if something weird
            img = img[..., :3]

        # now treat as RGB
        img = color.rgb2gray(img)  # -> grayscale float
        img = img.astype(np.float32)
        return img

    # anything else
    raise ValueError(f"Unexpected image shape {img.shape} for path {path}")


def to_uint8(img: np.ndarray) -> np.ndarray:
    """
    Safely convert any float image to uint8 [0, 255] by normalization.
    Works even if img is not in [0, 1].
    """
    img = np.asarray(img, dtype=np.float32)
    minv = float(np.min(img))
    maxv = float(np.max(img))
    if maxv <= minv:
        return np.zeros_like(img, dtype=np.uint8)
    img = (img - minv) / (maxv - minv)  # -> [0, 1]
    img = np.clip(img, 0.0, 1.0)
    return (img * 255.0).astype(np.uint8)


def compute_glcm_features(img_uint8: np.ndarray, prefix: str) -> dict:
    """Compute GLCM features (contrast, energy, etc.) for one uint8 image."""
    img_uint8 = np.asarray(img_uint8, dtype=np.uint8)
    glcm = graycomatrix(
        img_uint8,
        distances=DISTANCES,
        angles=ANGLES,
        symmetric=True,
        normed=True,
    )
    feats = {}
    for prop in GLCM_PROPS:
        vals = graycoprops(glcm, prop)  # shape (len(dist), len(angles))
        feats[f"{prefix}_{prop}"] = float(vals.mean())
    return feats


def find_image_files(root: str):
    """Yield full paths to image files under root."""
    exts = (".jpg", ".jpeg", ".png", ".bmp")
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if f.lower().endswith(exts):
                yield os.path.join(dirpath, f)


def process_single_image(path: str) -> dict:
    """
    Process a single image file:
    - load grayscale
    - apply entropy, gaussian, sobel, gabor, hessian, prewitt
    - compute GLCM features for each
    - return a flat dict with image_id, label, and numerical features
      OR an 'error' field if something went wrong.
    """
    lower = path.lower()
    if "/yes/" in lower or "\\yes\\" in lower:
        label = "yes"
    elif "/no/" in lower or "\\no\\" in lower:
        label = "no"
    else:
        label = "unknown"

    image_id = os.path.basename(path)

    try:
        img = load_gray(path)

        # base image uint8
        base_u8 = to_uint8(img)

        # filters (same style as instructor, but normalized later)
        entropy_img = entropy(base_u8, disk(2))                 # rank entropy needs uint8
        gaussian_img = nd.gaussian_filter(img, sigma=1)
        sobel_img = sobel(img)
        gabor_img = gabor(img, frequency=0.9)[1]                # imaginary part
        hessian_img = hessian(img, sigmas=range(1, 4, 1))
        prewitt_img = prewitt(img)

        # ensure everything is uint8 before GLCM
        entropy_u8 = to_uint8(entropy_img)
        gaussian_u8 = to_uint8(gaussian_img)
        sobel_u8 = to_uint8(sobel_img)
        gabor_u8 = to_uint8(gabor_img)
        if hessian_img.ndim > 2:
            hessian_img2d = hessian_img[..., 0]
        else:
            hessian_img2d = hessian_img
        hessian_u8 = to_uint8(hessian_img2d)
        prewitt_u8 = to_uint8(prewitt_img)

        feats = {
            "image_id": image_id,
            "label": label,
        }

        # GLCM features for each image/filter
        feats.update(compute_glcm_features(base_u8, "orig"))
        feats.update(compute_glcm_features(entropy_u8, "entropy"))
        feats.update(compute_glcm_features(gaussian_u8, "gaussian"))
        feats.update(compute_glcm_features(sobel_u8, "sobel"))
        feats.update(compute_glcm_features(gabor_u8, "gabor"))
        feats.update(compute_glcm_features(hessian_u8, "hessian"))
        feats.update(compute_glcm_features(prewitt_u8, "prewitt"))

        return feats

    except Exception as e:
        # return an error row – we'll filter these out later
        return {
            "image_id": image_id,
            "label": label,
            "error": str(e),
        }


def main(input_folder: str, output_parquet: str, num_workers: int = 4):
    start_time = time.time()

    image_paths = list(find_image_files(input_folder))
    if not image_paths:
        raise RuntimeError(f"No images found under {input_folder}")

    print(f"Found {len(image_paths)} images. Starting feature extraction...")

    rows = []
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        for row in tqdm(ex.map(process_single_image, image_paths), total=len(image_paths)):
            rows.append(row)

    df = pd.DataFrame(rows)

    # handle errors, if any
    if "error" in df.columns:
        num_errors = df["error"].notna().sum()
        print(f"Images with errors: {num_errors}")
        df_ok = df[df["error"].isna()].drop(columns=["error"])
    else:
        df_ok = df

    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    df_ok.to_parquet(output_parquet, index=False)

    num_images = df_ok.shape[0]
    num_features = max(num_images > 0 and df_ok.shape[1] - 2, 0)  # exclude image_id + label
    extraction_time_seconds = time.time() - start_time
    compute_sku = os.environ.get("AZUREML_COMPUTE", "local_or_unknown")

    print(f"num_images: {num_images}")
    print(f"num_features: {num_features}")
    print(f"extraction_time_seconds: {extraction_time_seconds:.2f}")
    print(f"compute_sku: {compute_sku}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_folder",
        type=str,
        required=True,
        help="Path to folder containing 'yes' and 'no' subfolders.",
    )
    parser.add_argument(
        "--output_parquet",
        type=str,
        required=True,
        help="Output Parquet file path for features.",
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        default=4,
        help="Number of parallel worker processes.",
    )
    args = parser.parse_args()

    main(
        input_folder=args.input_folder,
        output_parquet=args.output_parquet,
        num_workers=args.num_workers,
    )
