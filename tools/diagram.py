"""Render the README diagrams to docs/images/<name>-{light,dark}.png.

usage: CHROME=<path to chrome> python tools/diagram.py

Each diagram is drawn as SVG and photographed by a headless browser at twice its size, with a
transparent background, once per colour scheme.
"""

import os
import pathlib
import subprocess
import tempfile

OUT = pathlib.Path(__file__).resolve().parents[1] / "docs" / "images"
CHROME = os.environ.get("CHROME", "google-chrome")

THEMES = {
    "light": {"ink": "#1b1f23", "muted": "#5a6169", "line": "#c8ccd1", "fill": "#f5f6f7", "accent": "#2b6cb5"},
    "dark": {"ink": "#eceff2", "muted": "#9ba2aa", "line": "#3c434b", "fill": "#161b22", "accent": "#6aa9ee"},
}

MONO = "'JetBrains Mono', Consolas, ui-monospace, monospace"
SANS = "'Segoe UI', system-ui, -apple-system, Helvetica, Arial, sans-serif"


def box(t, x, y, w, h, title, lines, strong=False):
    stroke = t["accent"] if strong else t["line"]
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{t["fill"]}" stroke="{stroke}" '
        f'stroke-width="{2 if strong else 1.4}"/>',
        f'<text x="{x + 16}" y="{y + 30}" font-family="{MONO}" font-size="16" font-weight="700" '
        f'fill="{t["ink"]}">{title}</text>',
    ]
    for i, line in enumerate(lines):
        parts.append(
            f'<text x="{x + 16}" y="{y + 55 + i * 21}" font-family="{SANS}" font-size="13.5" '
            f'fill="{t["muted"]}">{line}</text>'
        )
    return "".join(parts)


def arrow(t, path, label="", lx=0, ly=0, anchor="middle", both=False):
    start = ' marker-start="url(#tail)"' if both else ""
    # An empty path is a second line of text for the arrow before it.
    parts = []
    if path:
        parts.append(
            f'<path d="{path}" fill="none" stroke="{t["muted"]}" stroke-width="1.6" marker-end="url(#head)"{start}/>'
        )
    if label:
        parts.append(
            f'<text x="{lx}" y="{ly}" text-anchor="{anchor}" font-family="{SANS}" font-size="12" '
            f'fill="{t["muted"]}">{label}</text>'
        )
    return "".join(parts)


def pill(t, x, y, w, text):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="40" rx="20" fill="{t["fill"]}" stroke="{t["ink"]}" '
        f'stroke-width="1.6"/>'
        f'<text x="{x + w / 2}" y="{y + 25}" text-anchor="middle" font-family="{SANS}" font-size="14" '
        f'font-weight="600" fill="{t["ink"]}">{text}</text>'
    )


def note(t, x, y, text):
    return f'<text x="{x}" y="{y}" font-family="{SANS}" font-size="13.5" fill="{t["muted"]}">{text}</text>'


def frame(t, w, h, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}"><defs>'
        '<marker id="head" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
        f'<path d="M0 0 L10 5 L0 10 z" fill="{t["muted"]}"/></marker>'
        '<marker id="tail" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
        f'<path d="M10 0 L0 5 L10 10 z" fill="{t["muted"]}"/></marker>'
        f"</defs>{body}</svg>"
    )


def node(t):
    """One node: who owns what, and what crosses each line."""
    w, h = 1040, 470
    body = [
        pill(t, 10, 60, 120, "client"),
        pill(t, 10, 150, 120, "peers"),
        box(t, 190, 20, 200, 210, "threads", ["accept connections", "one reader per connection", "one writer per peer", "", "bytes in, bytes out,", "nothing else"]),
        box(t, 460, 20, 250, 210, "Node", ["owned by one thread", "takes a batch of events,", "returns what to send", "", "decides nothing about", "consensus by itself"], True),
        box(t, 780, 20, 250, 210, "Raft core", ["a pure state machine:", "step(input) gives outputs", "no clock, no disk, no socket", "", "the simulator drives", "this same code"], True),
        box(t, 460, 300, 250, 110, "Storage", ["term, vote, log in katman", "one synced write per batch"]),
        box(t, 780, 300, 250, 110, "StateMachine", ["keys, sessions, membership", "in katman, not synced"]),
        arrow(t, "M130 80 H184", both=True),
        arrow(t, "M130 170 H184", both=True),
        arrow(t, "M390 100 H454", "events", 422, 92),
        arrow(t, "M454 150 H396", "effects", 422, 170),
        arrow(t, "M710 100 H774", "inputs", 742, 92),
        arrow(t, "M774 150 H716", "outputs", 742, 170),
        arrow(t, "M520 230 V294", "persist, then", 510, 258, "end"),
        arrow(t, "", "tell the core", 510, 275, "end"),
        arrow(t, "M650 230 V262 H905 V294", "apply what is committed, answer clients", 778, 254),
        note(t, 190, 445, "A write is on disk before the core hears of it, and the core sends nothing that depends on it until then."),
    ]
    return w, h, "".join(body)


def write(t):
    """One write, from the client's request to its answer."""
    w, h = 1040, 430
    cols = [("client", 70), ("leader", 300), ("leader's disk", 520), ("followers", 760), ("their disks", 960)]
    body = []
    for name, x in cols:
        body.append(
            f'<text x="{x}" y="28" text-anchor="middle" font-family="{MONO}" font-size="15" font-weight="700" '
            f'fill="{t["ink"]}">{name}</text>'
        )
        body.append(f'<path d="M{x} 42 V396" stroke="{t["line"]}" stroke-width="1.4" stroke-dasharray="4 5"/>')

    def step(y, a, b, text, strong=False):
        colour = t["accent"] if strong else t["muted"]
        mid = (a + b) / 2
        end = b - 6 if b > a else b + 6
        return (
            f'<path d="M{a} {y} H{end}" fill="none" stroke="{colour}" stroke-width="{2 if strong else 1.6}" '
            f'marker-end="url(#head)"/>'
            f'<text x="{mid}" y="{y - 8}" text-anchor="middle" font-family="{SANS}" font-size="13" '
            f'fill="{t["ink"] if strong else t["muted"]}">{text}</text>'
        )

    body += [
        step(80, 70, 300, "put k v  (session, seq)"),
        step(125, 300, 520, "append, sync", True),
        step(170, 520, 300, "durable"),
        step(215, 300, 760, "AppendEntries"),
        step(260, 760, 960, "append, sync", True),
        step(305, 960, 760, "durable"),
        step(350, 760, 300, "stored up to index n"),
        step(395, 300, 70, "done"),
        note(t, 318, 150, ""),
        f'<text x="310" y="196" font-family="{SANS}" font-size="12" fill="{t["muted"]}">held until here</text>',
        f'<text x="770" y="331" font-family="{SANS}" font-size="12" fill="{t["muted"]}">held until here</text>',
        f'<text x="310" y="376" font-family="{SANS}" font-size="12" fill="{t["muted"]}">a majority has it: commit, apply</text>',
    ]
    return w, h, "".join(body)


def simulator(t):
    """What the simulator holds and what it does to it."""
    w, h = 1040, 330
    body = [
        box(t, 10, 20, 250, 175, "one seed", ["cluster size, batch size,", "fault rates, every random", "choice after that", "", "a failing seed replays"], True),
        box(t, 330, 20, 330, 175, "one thread, one clock", ["nodes: the real core, the real", "storage, the real state machine", "network: a queue it can reorder", "disks: writes that finish later", "clients: lose, retry, repeat"]),
        box(t, 730, 20, 300, 175, "faults", ["drop, duplicate, delay, reorder", "partitions, a leader cut off", "slow disks, power cuts", "clocks that drift", "members that come and go"]),
        box(t, 10, 220, 500, 90, "after every step", ["one leader per term, same entries applied everywhere,", "leaders hold what is committed, nothing applied before its write"]),
        box(t, 530, 220, 500, 90, "at the end", ["one log with everything ever applied, a linearizable history", "per key, identical state machines"]),
        arrow(t, "M260 108 H324"),
        arrow(t, "M724 108 H666"),
    ]
    return w, h, "".join(body)


DIAGRAMS = {"node": node, "write": write, "simulator": simulator}

OUT.mkdir(parents=True, exist_ok=True)
for name, draw in DIAGRAMS.items():
    for scheme, theme in THEMES.items():
        width, height, body = draw(theme)
        page = pathlib.Path(tempfile.mkdtemp()) / f"{name}-{scheme}.html"
        page.write_text(
            '<!doctype html><html><head><meta charset="utf-8">'
            "<style>html,body{margin:0;background:transparent}svg{display:block}</style>"
            f"</head><body>{frame(theme, width, height, body)}</body></html>",
            encoding="utf-8",
        )
        target = OUT / f"{name}-{scheme}.png"
        # CHROME comes from the person running this script, not from untrusted input.
        subprocess.run(
            [
                CHROME,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                "--force-device-scale-factor=2",
                f"--window-size={width},{height}",
                "--default-background-color=00000000",
                f"--screenshot={target}",
                page.resolve().as_uri(),
            ],
            check=True,
            capture_output=True,
        )
        print(target.name)
