"""QR-код реферальной ссылки для экрана «Рефералка» в боте."""
from __future__ import annotations

from io import BytesIO

import qrcode


def referral_link_qr_png(url: str) -> bytes:
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()
