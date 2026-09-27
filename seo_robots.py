"""Bounded robots.txt parsing for the audit crawler, with no network access.

Rules follow RFC 9309 and Google's documented group/rule precedence. This is
not a Googlebot emulator: crawl-delay is an optional extension, returned in
seconds so the caller can defer crawling rather than shorten a long delay.
"""

from dataclasses import dataclass
import math
import re
from urllib.parse import quote, urlsplit


MAX_ROBOTS_BYTES = 500 * 1024
_UNRESERVED = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~")
_LITERAL_SAFE = _UNRESERVED | frozenset("/:?&=+;,@!()[]'")
_HEX = frozenset("0123456789abcdefABCDEF")


def _normalize(value: str, *, pattern: bool = False) -> str:
    """Normalize UTF-8 and percent escapes without decoding reserved octets."""
    result = []
    index = 0
    while index < len(value):
        char = value[index]
        if (char == "%" and index + 2 < len(value)
                and value[index + 1] in _HEX and value[index + 2] in _HEX):
            byte = int(value[index + 1:index + 3], 16)
            result.append(chr(byte) if chr(byte) in _UNRESERVED else f"%{byte:02X}")
            index += 3
            continue
        if char in _LITERAL_SAFE or (pattern and char == "*"):
            result.append(char)
        else:
            result.append(quote(char, safe=""))
        index += 1
    return "".join(result)


@dataclass(frozen=True)
class _Rule:
    parts: tuple[str, ...]
    anchored: bool
    length: int
    allowed: bool

    def matches(self, target: str) -> bool:
        # Searching literal pieces in order avoids regex wildcard backtracking.
        if not target.startswith(self.parts[0]):
            return False
        position = len(self.parts[0])
        for index, part in enumerate(self.parts[1:], start=1):
            if self.anchored and index == len(self.parts) - 1:
                return target.endswith(part) and len(target) - len(part) >= position
            found = target.find(part, position)
            if found == -1:
                return False
            position = found + len(part)
        return not self.anchored or position == len(target)


class RobotsPolicy:
    """Evaluate one origin's robots.txt for a crawler product token.

    Only the first 500 KiB of UTF-8 text are parsed; ``truncated`` reports any
    discarded content. Equally specific agent groups are merged. URL paths and
    queries are case-sensitive, fragments are ignored, and the longest matching
    rule wins, with Allow winning ties. ``crawl_delay`` is the largest finite,
    nonnegative delay in applicable groups, or None. It is never silently capped;
    callers must stop/defer if that delay exceeds their execution budget.

    The caller handles HTTP status, origin scope, fetching and crawl scheduling.
    """

    def __init__(self, text: str, user_agent: str = "mcp-seo-audit"):
        # Slice characters first to bound the temporary UTF-8 allocation too.
        encoded = text[:MAX_ROBOTS_BYTES + 1].encode("utf-8")
        self.truncated = len(encoded) > MAX_ROBOTS_BYTES
        text = encoded[:MAX_ROBOTS_BYTES].decode("utf-8", errors="ignore").lstrip("\ufeff")
        groups = []
        agents, rules, delays = [], [], []
        has_rule = False
        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            field, value = (piece.strip() for piece in line.split(":", 1))
            field = field.lower()
            if field == "user-agent":
                if has_rule:
                    groups.append((agents, rules, delays))
                    agents, rules, delays = [], [], []
                    has_rule = False
                # Ignore suffixes such as /1.0 and * after a product token.
                token = re.match(r"[a-z_-]+", value.lower())
                if value == "*":
                    agents.append("*")
                elif token:
                    agents.append(token.group())
            elif agents and field in {"allow", "disallow"}:
                has_rule = True
                if not value or value[0] not in "/*":
                    continue
                if any(ord(char) < 32 or ord(char) == 127 for char in value):
                    continue
                anchored = value.endswith("$")
                path = value[:-1] if anchored else value.rstrip("*")
                pattern = _normalize(path, pattern=True)
                rules.append(_Rule(tuple(pattern.split("*")), anchored,
                                   len(pattern) + int(anchored), field == "allow"))
            elif agents and field == "crawl-delay":
                # Other records must not terminate a robots rule group.
                try:
                    delay = float(value)
                except ValueError:
                    continue
                if math.isfinite(delay) and delay >= 0:
                    delays.append(delay)
        if agents:
            groups.append((agents, rules, delays))

        crawler = user_agent.lower()
        selected = []
        best = -1
        for agents, rules, delays in groups:
            specificity = max((0 if agent == "*" else len(agent)
                               for agent in agents if agent == "*" or agent in crawler), default=-1)
            if specificity > best:
                best, selected = specificity, [(rules, delays)]
            elif specificity == best and specificity >= 0:
                selected.append((rules, delays))
        self._rules = [rule for rules, _ in selected for rule in rules]
        self.crawl_delay = max((delay for _, delays in selected for delay in delays), default=None)

    def can_fetch(self, url: str) -> bool:
        """Return whether the supplied URL or absolute path is crawlable."""
        parsed = urlsplit(url)
        path = _normalize(parsed.path or "/")
        has_query = "?" in url.split("#", 1)[0]
        if path == "/robots.txt" and not has_query:
            return True
        target = path + ("?" + _normalize(parsed.query) if has_query else "")
        matching = (rule for rule in self._rules if rule.matches(target))
        winner = max(matching, key=lambda rule: (rule.length, rule.allowed), default=None)
        return winner.allowed if winner else True
