#!/usr/bin/env python3
"""Generate a letter-size, print-ready APRILTAG_25H9 marker PDF."""

from __future__ import annotations

import argparse
import binascii
import subprocess
import tempfile
from pathlib import Path

import cv2


LETTER_WIDTH_PT = 612
LETTER_HEIGHT_PT = 792
MARKER_SIZE_PT = 504  # 7 inches; leaves a 0.75-inch side margin.
MARKER_X_PT = (LETTER_WIDTH_PT - MARKER_SIZE_PT) // 2
MARKER_Y_PT = (LETTER_HEIGHT_PT - MARKER_SIZE_PT) // 2
MARKER_PIXELS = 700  # Integral 100 dpi when printed at 7 inches.


def ps_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def marker_hex(dictionary, marker_id: int) -> str:
    marker = cv2.aruco.drawMarker(dictionary, marker_id, MARKER_PIXELS, borderBits=1)
    return binascii.hexlify(marker.tobytes()).decode("ascii")


def page_ps(marker_id: int, marker_data: str) -> str:
    label = f"APRILTAG_25H9  |  ID {marker_id}"
    return f"""%%Page: {marker_id} {marker_id}
gsave
/Helvetica-Bold findfont 16 scalefont setfont
306 760 moveto ({ps_escape(label)}) dup stringwidth pop 2 div neg 0 rmoveto show
{MARKER_X_PT} {MARKER_Y_PT} translate
{MARKER_SIZE_PT} {MARKER_SIZE_PT} scale
{MARKER_PIXELS} {MARKER_PIXELS} 8
[{MARKER_PIXELS} 0 0 -{MARKER_PIXELS} 0 {MARKER_PIXELS}]
/markerrow {MARKER_PIXELS} string def
{{currentfile markerrow readhexstring pop}} image
{marker_data}
grestore
/Helvetica findfont 10 scalefont setfont
306 22 moveto (Print at Actual Size / 100%; do not fit or scale.) dup stringwidth pop 2 div neg 0 rmoveto show
showpage
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=13)
    parser.add_argument("--end", type=int, default=33)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("apriltag_25h9_ids_13_to_33_letter.pdf"),
    )
    args = parser.parse_args()
    if args.start < 0 or args.end < args.start:
        parser.error("Require non-negative IDs with --end >= --start.")

    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_APRILTAG_25h9)
    ps = [
        "%!PS-Adobe-3.0",
        "%%DocumentPaperSizes: Letter",
        "%%Pages: (atend)",
        "%%BoundingBox: 0 0 612 792",
        "%%EndComments",
    ]
    for marker_id in range(args.start, args.end + 1):
        ps.append(page_ps(marker_id, marker_hex(dictionary, marker_id)))
    ps.append(f"%%Trailer\n%%Pages: {args.end - args.start + 1}\n%%EOF")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".ps", delete=False) as ps_file:
        ps_file.write("\n".join(ps))
        ps_path = Path(ps_file.name)
    try:
        subprocess.run(
            [
                "gs",
                "-q",
                "-dSAFER",
                "-dBATCH",
                "-dNOPAUSE",
                "-sDEVICE=pdfwrite",
                "-sPAPERSIZE=letter",
                "-dCompatibilityLevel=1.4",
                f"-sOutputFile={args.output}",
                str(ps_path),
            ],
            check=True,
        )
    finally:
        ps_path.unlink(missing_ok=True)
    print(f"Wrote {args.output} ({args.start}–{args.end}, APRILTAG_25H9).")


if __name__ == "__main__":
    main()
