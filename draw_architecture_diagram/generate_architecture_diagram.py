# -*- coding: utf-8 -*-
"""
Credit Risk Monitoring Pipeline - Architecture Diagram Generator
Generates a beautiful tech stack + data flow diagram with official logos.
Run: python generate_architecture_diagram.py
Requirements: pip install pillow requests
"""

import os
import io
import math
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
    import requests
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pillow", "requests"])
    from PIL import Image, ImageDraw, ImageFont
    import requests

HAS_CAIRO = False  # cairosvg not available without GTK on Windows

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
OUTPUT_FILE = Path(__file__).parent / "architecture_diagram.png"
LOGO_CACHE  = Path(__file__).parent / ".logo_cache"
LOGO_CACHE.mkdir(exist_ok=True)

WIDTH  = 2180
HEIGHT = 860
BG_COLOR   = (13, 17, 23)
CARD_COLOR = (22, 27, 34)

# Tech nodes: (id, label, sublabel, color_hex, logo_url)
# Using reliable PNG CDNs
NODES = [
    ("hmda",      "HMDA / CFPB",     "12M+ rows/year",      "#4EC9B0",
     "https://img.icons8.com/color/96/bank-building.png"),

    ("fred",      "FRED API",        "Fed Funds Rate",       "#4EC9B0",
     "https://img.icons8.com/fluency/96/bonds.png"),

    ("airflow",   "Apache Airflow",  "Orchestration",        "#017CEE",
     "https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png"),

    ("pyspark_in", "PySpark",         "Ingest & Convert",    "#E25A1C",
     "https://raw.githubusercontent.com/apache/spark-website/master/site/images/spark-logo-trademark.png"),

    ("s3_raw",    "AWS S3 (Raw)",    "hmda/ & fred_raw/",    "#FF9900",
     "https://img.icons8.com/color/96/amazon-web-services.png"),

    ("spark",     "PySpark",         "Feature Engineering",  "#E25A1C",
     "https://raw.githubusercontent.com/apache/spark-website/master/site/images/spark-logo-trademark.png"),

    ("s3_stg",    "AWS S3 (Staging)","enriched_parquet/",    "#FF9900",
     "https://img.icons8.com/color/96/amazon-web-services.png"),

    ("postgres",  "PostgreSQL",      "Local / Dev",          "#336791",
     "https://www.postgresql.org/media/img/about/press/elephant.png"),

    ("snowflake", "Snowflake",       "Cloud / Prod",         "#29B5E8",
     "https://companieslogo.com/img/orig/SNOW-35164165.png"),

    ("dbt",       "dbt",             "SQL Modeling",         "#FF694B",
     "https://seeklogo.com/images/D/dbt-logo-500AB0BAA7-seeklogo.com.png"),

    ("streamlit", "Streamlit",       "Risk Dashboard",       "#FF4B4B",
     "https://streamlit.io/images/brand/streamlit-mark-color.png"),

    ("terraform", "Terraform",       "IaC S3 + Snowflake",   "#7B42BC",
     "https://img.icons8.com/color/96/terraform.png"),

    ("docker",    "Docker Compose",  "Containerization",     "#2496ED",
     "https://img.icons8.com/color/96/docker.png"),

    ("ec2",       "AWS EC2",         "m7i-flex.large",       "#FF9900",
     "https://img.icons8.com/color/96/amazon-web-services.png"),
]

# Node positions (cx, cy)
POSITIONS = {
    "hmda":      (180,  290),
    "fred":      (180,  510),
    "airflow":   (900,  88),   # rendered as banner, not card
    "pyspark_in":(430,  400),
    "s3_raw":    (690,  400),
    "spark":     (950,  400),
    "s3_stg":    (1210, 400),
    "postgres":  (1470, 270),
    "snowflake": (1470, 530),
    "dbt":       (1730, 400),
    "streamlit": (1990, 400),
    # Infra row
    "terraform": (1470, 710),
    "docker":    (950,  710),
    "ec2":       (1210, 710),
}

EDGES = [
    ("hmda",      "pyspark_in", "Download",       "main"),
    ("fred",      "pyspark_in", "API Pull",       "main"),
    ("pyspark_in","s3_raw",    "Upload",         "main"),
    ("s3_raw",    "spark",     "DAG 2",          "main"),
    ("spark",     "s3_stg",   "Enriched",       "main"),
    ("s3_stg",    "postgres",  "DAG 3 Opt-A",   "alt"),
    ("s3_stg",    "snowflake", "DAG 3 Opt-B",   "alt"),
    ("postgres",  "dbt",       "DAG 4",          "main"),
    ("snowflake", "dbt",       "DAG 4",          "main"),
    ("dbt",       "streamlit", "Marts",          "main"),
]

EDGE_COLORS = {
    "main":  (99,  210, 255),
    "alt":   (255, 185,  83),
    "infra": (140, 140, 160),
}

LAYER_LABELS = [
    (180,  150, "DATA SOURCES",    (78, 201, 176)),
    (430,  150, "INGESTION",       (86, 156, 214)),
    (690,  150, "DATA LAKE RAW",   (255, 153,   0)),
    (950,  150, "PROCESSING",      (226,  90,  28)),
    (1210, 150, "DATA LAKE STG",   (255, 153,   0)),
    (1470, 150, "DATA WAREHOUSE",  (100, 180, 255)),
    (1730, 150, "TRANSFORM",       (255, 105,  75)),
    (1990, 150, "DASHBOARD",       (255,  75,  75)),
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def hex_to_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def get_font(size, bold=False):
    candidates = []
    if bold:
        candidates = [
            "C:/Windows/Fonts/arialbd.ttf",
            "C:/Windows/Fonts/calibrib.ttf",
            "C:/Windows/Fonts/verdanab.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]
    else:
        candidates = [
            "C:/Windows/Fonts/arial.ttf",
            "C:/Windows/Fonts/calibri.ttf",
            "C:/Windows/Fonts/verdana.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
    return ImageFont.load_default()


def resize_pad(img, size):
    """Resize maintaining aspect ratio and pad to square."""
    img.thumbnail((size, size), Image.LANCZOS)
    canvas = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    paste_x = (size - img.width) // 2
    paste_y = (size - img.height) // 2
    canvas.paste(img, (paste_x, paste_y))
    return canvas


def download_logo(url, size=72):
    """Download logo (PNG or SVG), cache it, return RGBA PIL image."""
    cache_key = url.split("/")[-1].replace("?", "_")
    # Use .png extension for cache to avoid re-download
    png_cache = LOGO_CACHE / (cache_key.rsplit(".", 1)[0] + ".png")
    raw_cache = LOGO_CACHE / cache_key

    if png_cache.exists():
        try:
            img = Image.open(png_cache).convert("RGBA")
            return resize_pad(img, size)
        except Exception:
            png_cache.unlink(missing_ok=True)

    # Download raw file
    if not raw_cache.exists():
        try:
            resp = requests.get(url, timeout=12,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            raw_cache.write_bytes(resp.content)
        except Exception as exc:
            print(f"    [WARN] Could not download {url}: {exc}")
            return None

    raw = raw_cache.read_bytes()

    # Try direct PIL open first
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGBA")
        if "AirflowLogo.png" in url:
            img = img.crop((0, 0, img.height, img.height))
        img = resize_pad(img, size)
        img.save(png_cache, "PNG")
        return img
    except Exception:
        pass

    # If SVG, try cairosvg
    if HAS_CAIRO and (url.endswith(".svg") or b"<svg" in raw[:512]):
        try:
            png_bytes = cairosvg.svg2png(bytestring=raw,
                                         output_width=size, output_height=size)
            img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
            img.save(png_cache, "PNG")
            return img
        except Exception as exc:
            print(f"    [WARN] cairosvg failed for {cache_key}: {exc}")

    print(f"    [WARN] Could not render logo: {cache_key}")
    return None


def draw_arrow(canvas, draw, p1, p2, color, width=3, label="", font=None):
    x1, y1 = p1
    x2, y2 = p2
    draw.line([(x1, y1), (x2, y2)], fill=color, width=width)

    angle = math.atan2(y2 - y1, x2 - x1)
    alen, aang = 15, math.radians(28)
    ax1 = x2 - alen * math.cos(angle - aang)
    ay1 = y2 - alen * math.sin(angle - aang)
    ax2 = x2 - alen * math.cos(angle + aang)
    ay2 = y2 - alen * math.sin(angle + aang)
    draw.polygon([(x2, y2), (int(ax1), int(ay1)), (int(ax2), int(ay2))], fill=color)

    if label and font:
        mx, my = (x1 + x2) // 2, (y1 + y2) // 2
        bbox = draw.textbbox((0, 0), label, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        pad_x, pad_y = 8, 4
        tmp = Image.new("RGBA", (tw + pad_x * 2, th + pad_y * 2), (0, 0, 0, 0))
        td  = ImageDraw.Draw(tmp)
        td.rounded_rectangle([0, 0, tw + pad_x * 2, th + pad_y * 2],
                             radius=4, fill=(28, 33, 44, 215))
        td.text((pad_x, pad_y), label, font=font, fill=(*color, 230))
        px = mx - (tw + pad_x * 2) // 2
        py = my - (th + pad_y * 2) // 2
        canvas.paste(tmp, (px, py), tmp)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def build_diagram():
    print("[*] Credit Risk Monitoring Pipeline - Architecture Diagram")
    print("[*] Downloading logos...")

    node_dict = {nid: (label, sub, col, url)
                 for nid, label, sub, col, url in NODES}

    logo_map = {}
    for nid, label, sub, col, url in NODES:
        print(f"    -> {label}")
        logo_map[nid] = download_logo(url, size=72)

    print("[*] Rendering diagram...")

    # Canvas
    canvas = Image.new("RGBA", (WIDTH, HEIGHT), (*BG_COLOR, 255))
    draw   = ImageDraw.Draw(canvas, "RGBA")

    # Subtle gradient top→bottom
    for y in range(HEIGHT):
        t = y / HEIGHT
        r = int(BG_COLOR[0] * (1 - t) + 18 * t)
        g = int(BG_COLOR[1] * (1 - t) + 22 * t)
        b = int(BG_COLOR[2] * (1 - t) + 38 * t)
        draw.line([(0, y), (WIDTH, y)], fill=(r, g, b))

    # Fonts
    f_layer  = get_font(15, bold=True)
    f_label  = get_font(16, bold=True)
    f_small  = get_font(13)
    f_edge   = get_font(13)
    f_dag    = get_font(13)
    f_banner = get_font(20, bold=True)

    # ── Airflow Banner ─────────────────────────────────────────────────────────
    af_cx, af_cy = POSITIONS["airflow"]
    bw, bh = 1560, 56
    bx0, by0 = af_cx - bw // 2, af_cy - bh // 2
    bx1, by1 = bx0 + bw, by0 + bh
    draw.rounded_rectangle([bx0, by0, bx1, by1], radius=10,
                           fill=(1, 45, 100, 220),
                           outline=(0, 100, 210, 210), width=2)

    af_logo = logo_map.get("airflow")
    ls = 46
    if af_logo:
        tmp_af = af_logo.resize((ls, ls), Image.LANCZOS)
        canvas.paste(tmp_af, (bx0 + 12, by0 + (bh - ls) // 2), tmp_af)

    draw.text((bx0 + 72, by0 + bh // 2),
              "Apache Airflow  |  DAG Orchestration & Automated Chaining",
              font=f_banner, fill=(90, 180, 255), anchor="lm")
              
    # Env badge for Airflow
    af_env = "Docker Compose / EC2"
    bbox = draw.textbbox((0, 0), af_env, font=f_dag)
    ew, eh = bbox[2] - bbox[0], bbox[3] - bbox[1]
    icon_sz = 14
    gap = 6
    tot_w = icon_sz + gap + ew

    hx = bx0 + 30 + tot_w // 2
    hy = by0 - 10
    draw.rounded_rectangle([hx - tot_w//2 - 8, hy - eh//2 - 4, hx + tot_w//2 + 8, hy + eh//2 + 4],
                           radius=4, fill=(40, 50, 70, 240), outline=(100, 150, 255, 180), width=1)
    
    bi = logo_map.get("docker")
    if bi:
        bi_r = bi.resize((icon_sz, icon_sz), Image.LANCZOS)
        canvas.paste(bi_r, (int(hx - tot_w//2), int(hy - icon_sz//2)), bi_r)

    draw.text((hx - tot_w//2 + icon_sz + gap, hy), af_env, font=f_dag, fill=(180, 220, 255), anchor="lm")

    # DAG boxes
    dag_defs = [
        ("DAG 1: Ingest",    (0, 95, 200)),
        ("DAG 2: Transform", (0, 135, 175)),
        ("DAG 3: Load DW",   (0, 155, 130)),
        ("DAG 4: dbt Build", (170, 75, 25)),
    ]
    dw, dh = 150, 36
    dx_start = bx0 + 640
    gap = 24
    f_dag_box = get_font(13, bold=True)
    for i, (dlbl, dcol) in enumerate(dag_defs):
        dx0 = dx_start + i * (dw + gap)
        dy0 = by0 + (bh - dh) // 2
        draw.rounded_rectangle([dx0, dy0, dx0 + dw, dy0 + dh],
                               radius=6, fill=(*dcol, 215),
                               outline=(200, 220, 255, 120), width=1)
        draw.text((dx0 + dw // 2, dy0 + dh // 2), dlbl,
                  font=f_dag_box, fill=(255, 255, 255), anchor="mm")
        if i < 3:
            arr_x = dx0 + dw + gap // 2
            arr_y = dy0 + dh // 2
            draw.polygon([(arr_x - 5, arr_y - 6), (arr_x + 5, arr_y), (arr_x - 5, arr_y + 6)], fill=(140, 200, 255))

    # ── Layer labels ───────────────────────────────────────────────────────────
    for lx, ly, lname, lcol in LAYER_LABELS:
        bbox = draw.textbbox((0, 0), lname, font=f_layer)
        tw = bbox[2] - bbox[0]
        draw.line([(lx - tw // 2 - 6, ly + 16), (lx + tw // 2 + 6, ly + 16)],
                 fill=(*lcol, 100), width=1)
        draw.text((lx, ly), lname, font=f_layer, fill=(*lcol, 190), anchor="mt")

    # Infra separator
    draw.line([(60, 630), (WIDTH - 60, 630)], fill=(42, 48, 70), width=1)
    draw.text((80, 638), "INFRASTRUCTURE LAYER",
              font=f_layer, fill=(120, 120, 160, 200))

    # ── Edges ──────────────────────────────────────────────────────────────────
    bound_w, bound_h = 78, 54   # card bounding box for edge intersection

    for src, dst, elabel, estyle in EDGES:
        if src not in POSITIONS or dst not in POSITIONS:
            continue
        if src == "airflow" or dst == "airflow":
            continue
        scx, scy = POSITIONS[src]
        dcx, dcy = POSITIONS[dst]
        col = EDGE_COLORS.get(estyle, (180, 180, 180))
        lw  = 2 if estyle == "infra" else 3

        dx, dy = dcx - scx, dcy - scy
        if dx == 0 and dy == 0: continue

        t_x = bound_w / abs(dx) if dx != 0 else float('inf')
        t_y = bound_h / abs(dy) if dy != 0 else float('inf')
        t_dest = min(t_x, t_y)
        t_src  = min(t_x, t_y)

        sx = int(scx + dx * t_src)
        sy = int(scy + dy * t_src)
        ex = int(dcx - dx * t_dest)
        ey = int(dcy - dy * t_dest)

        draw_arrow(canvas, draw, (sx, sy), (ex, ey),
                   col, width=lw, label=elabel, font=f_edge)

    # ── Node Cards ─────────────────────────────────────────────────────────────
    cardw, cardh = 156, 114
    logo_sz = 46

    ENV_MAP = {
        "pyspark_in": "Docker Compose / EC2",
        "spark": "Docker Compose / EC2",
        "postgres": "Docker Compose / EC2 (Dev)",
        "dbt": "Docker Compose / EC2",
        "streamlit": "Docker Compose / EC2",
        "s3_raw": "Terraform",
        "s3_stg": "Terraform",
        "snowflake": "Terraform",
    }

    for nid, (cx, cy) in POSITIONS.items():
        if nid == "airflow":
            continue
        label, sub, color_hex, _ = node_dict[nid]
        rgb = hex_to_rgb(color_hex)

        x0, y0 = cx - cardw // 2, cy - cardh // 2
        x1, y1 = cx + cardw // 2, cy + cardh // 2

        # Drop shadow
        draw.rounded_rectangle([x0 + 5, y0 + 5, x1 + 5, y1 + 5],
                               radius=14, fill=(0, 0, 0, 80))
        # Card body
        draw.rounded_rectangle([x0, y0, x1, y1], radius=14,
                               fill=(*CARD_COLOR, 245),
                               outline=(*rgb, 175), width=2)
        # Top color accent stripe
        draw.rounded_rectangle([x0, y0, x1, y0 + 7], radius=14,
                               fill=(*rgb, 210))
        draw.rectangle([x0, y0 + 5, x1, y0 + 7], fill=(*rgb, 210))

        # Logo
        logo = logo_map.get(nid)
        if logo:
            lx = cx - logo_sz // 2
            ly = y0 + 16
            draw.ellipse([lx - 5, ly - 5, lx + logo_sz + 5, ly + logo_sz + 5],
                         fill=(255, 255, 255, 22))
            logo_r = logo.resize((logo_sz, logo_sz), Image.LANCZOS)
            canvas.paste(logo_r, (lx, ly), logo_r)

        # Text
        ty = y0 + 16 + logo_sz + 12
        draw.text((cx, ty), label, font=f_label, fill=(215, 228, 255), anchor="mt")
        draw.text((cx, ty + 24), sub, font=f_small, fill=(*rgb, 195), anchor="mt")

        # Env Badge
        env_text = ENV_MAP.get(nid)
        if env_text:
            hy = y0 - 10
            bbox = draw.textbbox((0,0), env_text, font=f_edge)
            ew, eh = bbox[2] - bbox[0], bbox[3] - bbox[1]
            icon_sz = 14
            gap = 6
            tot_w = icon_sz + gap + ew

            hx = cx
            draw.rounded_rectangle([hx - tot_w//2 - 6, hy - eh//2 - 4, hx + tot_w//2 + 6, hy + eh//2 + 4],
                                   radius=4, fill=(35, 45, 60, 220), outline=(80, 90, 110, 180), width=1)
            
            icon_key = "terraform" if "Terraform" in env_text else "docker"
            bi = logo_map.get(icon_key)
            if bi:
                bi_r = bi.resize((icon_sz, icon_sz), Image.LANCZOS)
                canvas.paste(bi_r, (int(hx - tot_w//2), int(hy - icon_sz//2)), bi_r)

            draw.text((hx - tot_w//2 + icon_sz + gap, hy), env_text, font=f_edge, fill=(160, 180, 200), anchor="lm")

    # ── Legend ─────────────────────────────────────────────────────────────────
    legend = [
        ("Main Data Pipeline Flow",               EDGE_COLORS["main"],  "---"),
        ("Alternative Data Warehouse Backend",    EDGE_COLORS["alt"],   "---"),
        ("Infrastructure Provisioning & Hosting", EDGE_COLORS["infra"], "..."),
    ]
    f_leg = get_font(15)
    lx0 = WIDTH - 340
    ly0 = HEIGHT - 110
    draw.rounded_rectangle([lx0 - 20, ly0 - 15, WIDTH - 20, HEIGHT - 20],
                           radius=10, fill=(20, 25, 38, 225),
                           outline=(55, 65, 95, 180), width=1)
    # Legend Title
    draw.text((lx0, ly0), "Legend", font=f_layer, fill=(160, 170, 200))
    for i, (lname, lcol, sym) in enumerate(legend):
        draw.text((lx0, ly0 + 26 + i * 22), f"{sym}  {lname}",
                 font=f_leg, fill=(*lcol, 210))

    # ── Footer ─────────────────────────────────────────────────────────────────
    draw.text((60, HEIGHT - 22),
              "(c) PhanNH - Credit Risk Monitoring Pipeline - Data Engineering Project",
              font=f_small, fill=(75, 85, 115))


    # ── Save ───────────────────────────────────────────────────────────────────
    out = canvas.convert("RGB")
    out.save(OUTPUT_FILE, "PNG", optimize=True)
    print(f"[OK] Diagram saved: {OUTPUT_FILE}")
    return str(OUTPUT_FILE)


if __name__ == "__main__":
    path = build_diagram()
    try:
        os.startfile(path)
    except Exception:
        pass
