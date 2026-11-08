import re
import hashlib

STOP_WORDS = [
    "реклама", "erid", "промокод", "розыгрыш", "ставк",
    "букмекер", "казино", "подписывайся", "подпишись"
]

URL_RE = re.compile(r"https?://\S+|t\.me/\S+|@[a-zA-Z0-9_]+")
SPACE_RE = re.compile(r"\n{3,}")


def is_ad_or_spam(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in STOP_WORDS)


def clean_text(text: str) -> str:
    cleaned = URL_RE.sub("", text)
    cleaned = SPACE_RE.sub("\n\n", cleaned)
    return cleaned.strip()


def calc_simhash(text: str) -> int:
    # бьем текст на шинглы по 3 слова для нормального отлова рерайтов
    words = re.findall(r"[а-яa-z0-9]{3,}", text.lower())
    if not words:
        return 0

    shingles = [
        " ".join(words[i : i + 3]) for i in range(max(1, len(words) - 2))
    ]
    v = [0] * 63

    # считаем до 63 бит, т.к. стандартный bigint в postgres знаковый (до 2^63 - 1)
    for sh in shingles:
        digest = hashlib.md5(sh.encode("utf-8")).digest()
        h = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
        for i in range(63):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1

    ans = 0
    for i in range(63):
        if v[i] > 0:
            ans |= 1 << i
    return ans


def is_near_duplicate(h1: int, h2: int, max_dist: int = 3) -> bool:
    return (h1 ^ h2).bit_count() <= max_dist