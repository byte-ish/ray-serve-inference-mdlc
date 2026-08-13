#!/usr/bin/env python3
"""Render a markdown document as a self-contained, themed GitHub Pages site.

Copy this into the target repo as `build.py` so regenerating the site does not
depend on the skill directory.

    python3 build.py REPORT.md docs/index.html \
        --title "Report title" \
        --subtitle "One line under the title" \
        --badge "Verdict: X" --badge "39 sources" \
        --repo owner/name

Requires pandoc. Beyond straight conversion it applies reader-facing transforms:
inline [n] citations become links into the bibliography, confidence tags such as
[verified] become chips, ratings like the filled/hollow circle meters stop
wrapping, headings gain anchors, and every table gets a horizontal scroll
container. See references/preflight.md for why each one exists.
"""
from __future__ import annotations

import argparse
import html
import pathlib
import re
import subprocess
import sys

RATING_LABELS = {"●●●": "Strong",
                 "●●○": "Adequate",
                 "●○○": "Weak"}


# --------------------------------------------------------------------------- #
# conversion
# --------------------------------------------------------------------------- #

def markdown_to_html(path: pathlib.Path) -> str:
    try:
        proc = subprocess.run(
            ["pandoc", str(path), "-f", "gfm", "-t", "html5",
             "--syntax-highlighting=none", "--wrap=none"],
            capture_output=True, text=True, check=True,
        )
    except FileNotFoundError:
        sys.exit("pandoc not found. Install it: brew install pandoc")
    except subprocess.CalledProcessError as exc:
        sys.exit(f"pandoc failed:\n{exc.stderr}")
    return proc.stdout


def shield(body: str):
    """Hide code spans/blocks so text transforms never touch them."""
    store: list[str] = []

    def keep(m):
        store.append(m.group(0))
        return f"\x00{len(store) - 1}\x00"

    body = re.sub(r"<pre>.*?</pre>", keep, body, flags=re.S)
    body = re.sub(r"<code>.*?</code>", keep, body, flags=re.S)
    return body, store


def unshield(body: str, store) -> str:
    return re.sub(r"\x00(\d+)\x00", lambda m: store[int(m.group(1))], body)


# --------------------------------------------------------------------------- #
# transforms
# --------------------------------------------------------------------------- #

def mark_sources(body: str):
    """Give the final <ol> (the bibliography) ids so citations can link to it.

    Pandoc emits <ol type="1">, not <ol>. Match attributes or this silently
    does nothing.
    """
    matches = list(re.finditer(r"<ol[^>]*>(.*?)</ol>", body, flags=re.S))
    if not matches:
        return body, 0
    last = matches[-1]
    n = [0]

    def number(_m):
        n[0] += 1
        return f'<li id="src-{n[0]}">'

    inner = re.sub(r"<li>", number, last.group(1))
    return body[:last.start()] + f'<ol class="sources" type="1">{inner}</ol>' + body[last.end():], n[0]


def link_citations(body: str, n_sources: int) -> str:
    def repl(m):
        num = int(m.group(1))
        if not 1 <= num <= n_sources:
            return m.group(0)
        return (f'<a class="cite" href="#src-{num}" '
                f'title="Jump to source {num}">{num}</a>')
    return re.sub(r"\[(\d{1,3})\]", repl, body)


def chip_tags(body: str) -> str:
    def repl(m):
        kind = {"verified": "ok", "approx": "soft", "dated": "soft"}.get(
            m.group(1).lower(), "warn")
        return f'<span class="tag t-{kind}">{html.escape(m.group(0)[1:-1])}</span>'
    return re.sub(r"\[(verified|approx|unverified|dated|verify)\b[^\]]*\]",
                  repl, body, flags=re.I)


def style_ratings(body: str) -> str:
    def repl(m):
        label = RATING_LABELS.get(m.group(0))
        title = f' title="{label}"' if label else ""
        return f'<span class="rate"{title}>{m.group(0)}</span>'
    return re.sub(r"[●○]{2,3}", repl, body)


def tidy_rules(body: str) -> str:
    """Drop <hr> immediately before <h2>; the heading already rules itself."""
    return re.sub(r"<hr\s*/?>\s*(?=<h2)", "", body)


def slugify(text: str) -> str:
    s = html.unescape(re.sub(r"<[^>]+>", "", text)).lower()
    return re.sub(r"[\s_]+", "-", re.sub(r"[^\w\s-]", "", s)).strip("-")


def build_toc(body: str):
    entries: list[dict] = []

    def repl(m):
        level, attrs, text = m.group(1), m.group(2), m.group(3)
        slug = base = slugify(text)
        i = 1
        while any(e["slug"] == slug for e in entries):
            i += 1
            slug = f"{base}-{i}"
        entries.append({"level": int(level), "slug": slug, "text": text})
        return f'<h{level} id="{slug}"{attrs}>{text}</h{level}>'

    body = re.sub(r"<h([23])([^>]*?)>(.*?)</h\1>", repl, body, flags=re.S)
    body = re.sub(r'(<h[23] id="[^"]+")\s+id="[^"]*"', r"\1", body)
    items = [
        f'<a class="t{e["level"]}" href="#{e["slug"]}">'
        f'{re.sub(r"<[^>]+>", "", e["text"])}</a>'
        for e in entries
    ]
    return "\n".join(items), body


def anchor_headings(body: str) -> str:
    return re.sub(
        r'(<h([23]) id="([^"]+)"[^>]*>)(.*?)(</h\2>)',
        lambda m: (f'{m.group(1)}{m.group(4)}'
                   f'<a class="anchor" href="#{m.group(3)}" '
                   f'aria-label="Link to this section">#</a>{m.group(5)}'),
        body, flags=re.S)


def wrap_tables(body: str) -> str:
    return (body.replace("<table>", '<div class="tw"><table>')
                .replace("</table>", "</table></div>"))


# --------------------------------------------------------------------------- #
# presentation
# --------------------------------------------------------------------------- #

CSS = """
:root{
  --paper:#F3F5F4; --card:#FFF; --ink:#16211E; --muted:#54615C; --faint:#87918D;
  --rule:#D9DFDC; --rule-strong:#BFC8C4; --a:#0E5C50; --a-soft:#0E5C5014;
  --warn:#8A6D1F; --warn-soft:#8A6D1F16; --ok-soft:#0E5C5016; --code-bg:#EAEFEC;
  --serif:'Iowan Old Style','Palatino Linotype',Palatino,'Book Antiqua',Georgia,serif;
  --sans:ui-sans-serif,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
  --mono:ui-monospace,SFMono-Regular,'SF Mono',Menlo,Consolas,monospace;
}
@media (prefers-color-scheme:dark){:root{
  --paper:#111614;--card:#171E1B;--ink:#E7EDEA;--muted:#A2ACA8;--faint:#78837F;
  --rule:#26302C;--rule-strong:#35413C;--a:#4FB3A0;--a-soft:#4FB3A016;
  --warn:#D2A945;--warn-soft:#D2A94518;--ok-soft:#4FB3A018;--code-bg:#1D2522;}}
:root[data-theme="dark"]{
  --paper:#111614;--card:#171E1B;--ink:#E7EDEA;--muted:#A2ACA8;--faint:#78837F;
  --rule:#26302C;--rule-strong:#35413C;--a:#4FB3A0;--a-soft:#4FB3A016;
  --warn:#D2A945;--warn-soft:#D2A94518;--ok-soft:#4FB3A018;--code-bg:#1D2522;}
:root[data-theme="light"]{
  --paper:#F3F5F4;--card:#FFF;--ink:#16211E;--muted:#54615C;--faint:#87918D;
  --rule:#D9DFDC;--rule-strong:#BFC8C4;--a:#0E5C50;--a-soft:#0E5C5014;
  --warn:#8A6D1F;--warn-soft:#8A6D1F16;--ok-soft:#0E5C5016;--code-bg:#EAEFEC;}

*{box-sizing:border-box;}
html{scroll-behavior:smooth;scroll-padding-top:28px;}
body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--sans);
  font-size:16.5px;line-height:1.72;-webkit-font-smoothing:antialiased;}

.masthead{border-bottom:1px solid var(--rule);background:var(--card);}
.masthead .inner{max-width:1280px;margin:0 auto;padding:36px 32px 32px;
  display:flex;flex-direction:column;gap:12px;}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;
  text-transform:uppercase;color:var(--faint);font-weight:600;}
.masthead h1{font-family:var(--serif);font-weight:600;margin:0;
  font-size:clamp(27px,3.6vw,40px);line-height:1.12;letter-spacing:-.015em;text-wrap:balance;}
.masthead .sub{color:var(--muted);max-width:68ch;margin:0;font-size:17px;line-height:1.6;}
.badges{display:flex;flex-wrap:wrap;gap:8px;margin-top:4px;}
.badge{font-family:var(--mono);font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;
  font-weight:600;padding:4px 9px;border-radius:2px;background:var(--a-soft);color:var(--a);}
.badge.n{background:transparent;color:var(--muted);border:1px solid var(--rule-strong);}

.shell{max-width:1280px;margin:0 auto;padding:0 32px;display:grid;
  grid-template-columns:262px minmax(0,1fr);gap:56px;align-items:start;}
@media (max-width:1000px){.shell{grid-template-columns:1fr;gap:0;padding:0 20px;}}

nav.toc{position:sticky;top:0;max-height:100vh;overflow-y:auto;padding:36px 0 40px;
  display:flex;flex-direction:column;gap:1px;}
@media (max-width:1000px){nav.toc{position:static;max-height:none;
  border-bottom:1px solid var(--rule);margin-bottom:10px;padding-bottom:22px;}}
nav.toc .toc-lbl{font-family:var(--mono);font-size:10.5px;letter-spacing:.14em;
  text-transform:uppercase;color:var(--faint);font-weight:600;margin-bottom:10px;}
nav.toc a{display:block;text-decoration:none;color:var(--muted);font-size:13.5px;
  line-height:1.45;padding:5px 10px;border-left:2px solid transparent;}
nav.toc a:hover{color:var(--ink);background:var(--a-soft);}
nav.toc a.t3{padding-left:24px;font-size:12.5px;color:var(--faint);}
nav.toc a.active{color:var(--a);border-left-color:var(--a);font-weight:600;background:var(--a-soft);}

main{padding:36px 0 110px;min-width:0;max-width:74ch;}
main h1{display:none;}
main h2{font-family:var(--serif);font-size:clamp(22px,2.6vw,28px);line-height:1.22;
  font-weight:600;margin:54px 0 18px;padding-top:18px;border-top:2px solid var(--ink);
  text-wrap:balance;}
main h2:first-of-type{margin-top:0;}
main h3{font-family:var(--serif);font-size:19.5px;font-weight:600;margin:38px 0 12px;
  line-height:1.32;text-wrap:balance;}
main p{margin:0 0 17px;}
main em{color:var(--muted);}

main ul,main ol{margin:0 0 20px;padding-left:0;}
main ul{list-style:none;}
main ul>li{position:relative;padding-left:22px;}
main ul>li::before{content:"";position:absolute;left:6px;top:.72em;width:5px;height:5px;
  border-radius:50%;background:var(--rule-strong);}
main ol{padding-left:30px;}
main ol>li{padding-left:6px;}
main ol>li::marker{color:var(--faint);font-family:var(--mono);font-size:.86em;font-weight:600;}
main li{margin-bottom:9px;line-height:1.68;}
main li>ul,main li>ol{margin:9px 0 4px;}

main hr{border:none;border-top:1px solid var(--rule);margin:38px 0;}
main a{color:var(--a);text-decoration:underline;text-underline-offset:2.5px;
  text-decoration-color:var(--rule-strong);}
main a:hover{text-decoration-color:var(--a);}

main blockquote{margin:0 0 22px;padding:16px 20px;border-left:3px solid var(--a);
  background:var(--a-soft);border-radius:0 3px 3px 0;}
main blockquote>*:last-child{margin-bottom:0;}

a.cite{display:inline-block;font-family:var(--mono);font-size:10.5px;font-weight:600;
  line-height:1;vertical-align:super;padding:2px 4px;margin:0 1px;border-radius:2px;
  background:var(--a-soft);color:var(--a);text-decoration:none;}
a.cite:hover{background:var(--a);color:var(--paper);}

.tag{font-family:var(--mono);font-size:10.5px;font-weight:600;padding:2px 6px;
  border-radius:2px;white-space:nowrap;}
.tag.t-ok{background:var(--ok-soft);color:var(--a);}
.tag.t-warn{background:var(--warn-soft);color:var(--warn);}
.tag.t-soft{background:transparent;color:var(--faint);border:1px solid var(--rule-strong);}
.rate{white-space:nowrap;letter-spacing:.09em;color:var(--a);font-size:.95em;cursor:help;}

ol.sources{padding-left:34px;}
ol.sources>li{margin-bottom:12px;line-height:1.6;font-size:15px;padding-left:4px;}
ol.sources a{overflow-wrap:anywhere;}
ol.sources>li:target{background:var(--a-soft);outline:2px solid var(--a);outline-offset:5px;}

code{font-family:var(--mono);font-size:.855em;background:var(--code-bg);
  padding:1.5px 5px;border-radius:2px;overflow-wrap:anywhere;}
pre{background:var(--card);border:1px solid var(--rule);border-radius:3px;
  padding:16px 18px;overflow-x:auto;margin:0 0 20px;line-height:1.55;}
pre code{background:none;padding:0;font-size:12.5px;}

.tw{overflow-x:auto;border:1px solid var(--rule);border-radius:3px;margin:0 0 22px;
  background:var(--card);}
table{border-collapse:collapse;width:100%;font-size:13.5px;line-height:1.55;}
th,td{text-align:left;padding:11px 14px;border-bottom:1px solid var(--rule);vertical-align:top;}
thead th{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;
  color:var(--faint);font-weight:600;background:var(--paper);white-space:nowrap;}
tbody tr:last-child td{border-bottom:none;}
tbody tr:hover{background:var(--a-soft);}
td{font-variant-numeric:tabular-nums;}
td:first-child{font-weight:550;}

.anchor{margin-left:9px;color:var(--rule-strong);text-decoration:none;font-family:var(--mono);
  font-size:.62em;opacity:0;transition:opacity .12s;}
h2:hover .anchor,h3:hover .anchor,.anchor:focus{opacity:1;}
.anchor:hover{color:var(--a);}

footer{border-top:1px solid var(--rule);margin-top:60px;padding-top:22px;font-size:13.5px;
  color:var(--faint);display:flex;flex-direction:column;gap:9px;}
footer a{color:var(--a);}
a:focus-visible{outline:2px solid var(--a);outline-offset:3px;}

.totop{position:fixed;right:22px;bottom:22px;background:var(--card);color:var(--muted);
  border:1px solid var(--rule-strong);border-radius:3px;padding:9px 13px;font-size:12px;
  font-family:var(--mono);letter-spacing:.06em;text-transform:uppercase;text-decoration:none;
  opacity:0;pointer-events:none;transition:opacity .18s;}
.totop.on{opacity:1;pointer-events:auto;}
.totop:hover{color:var(--a);border-color:var(--a);}

@media (prefers-reduced-motion:reduce){html{scroll-behavior:auto;}*{transition:none!important;}}
@media print{nav.toc,.totop{display:none;}.shell{grid-template-columns:1fr;}main{max-width:none;}}
"""

JS = """
(function(){
  var links={},heads=[];
  Array.prototype.forEach.call(document.querySelectorAll('nav.toc a'),function(a){
    var id=a.getAttribute('href').slice(1),el=document.getElementById(id);
    if(el){links[id]=a;heads.push(el);}
  });
  if(heads.length&&'IntersectionObserver' in window){
    var active=null;
    var obs=new IntersectionObserver(function(entries){
      entries.forEach(function(en){
        if(!en.isIntersecting)return;
        var a=links[en.target.id];
        if(!a||a===active)return;
        if(active)active.classList.remove('active');
        a.classList.add('active');active=a;
        if(a.scrollIntoView)a.scrollIntoView({block:'nearest'});
      });
    },{rootMargin:'0px 0px -78% 0px',threshold:0});
    heads.forEach(function(h){obs.observe(h);});
  }
  var btn=document.querySelector('.totop');
  if(btn){
    var tick=function(){btn.classList.toggle('on',window.scrollY>700);};
    window.addEventListener('scroll',tick,{passive:true});tick();
  }
})();
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", type=pathlib.Path)
    ap.add_argument("output", type=pathlib.Path)
    ap.add_argument("--title", required=True)
    ap.add_argument("--subtitle", default="")
    ap.add_argument("--eyebrow", default="")
    ap.add_argument("--badge", action="append", default=[],
                    help="repeatable; the first renders in the accent colour")
    ap.add_argument("--repo", default="", help="owner/name, for the footer link")
    args = ap.parse_args()

    if not args.source.exists():
        sys.exit(f"missing {args.source}")

    body = markdown_to_html(args.source)
    body, n_sources = mark_sources(body)

    body, store = shield(body)
    body = link_citations(body, n_sources)
    body = chip_tags(body)
    body = style_ratings(body)
    body = tidy_rules(body)
    body = unshield(body, store)

    toc, body = build_toc(body)
    body = anchor_headings(body)
    body = wrap_tables(body)

    badges = "\n    ".join(
        f'<span class="badge{"" if i == 0 else " n"}">{html.escape(b)}</span>'
        for i, b in enumerate(args.badge))
    eyebrow = (f'<span class="eyebrow">{html.escape(args.eyebrow)}</span>'
               if args.eyebrow else "")
    sub = f'<p class="sub">{html.escape(args.subtitle)}</p>' if args.subtitle else ""
    repo_line = (f'<p>Source: <a href="https://github.com/{args.repo}">'
                 f'github.com/{args.repo}</a></p>' if args.repo else "")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(args.title)}</title>
<meta name="description" content="{html.escape(args.subtitle)}">
<meta name="color-scheme" content="light dark">
<style>{CSS}</style>
</head>
<body>
<header class="masthead"><div class="inner">
  {eyebrow}
  <h1>{html.escape(args.title)}</h1>
  {sub}
  <div class="badges">
    {badges}
  </div>
</div></header>
<div class="shell">
  <nav class="toc" aria-label="Contents">
    <span class="toc-lbl">Contents</span>
    {toc}
  </nav>
  <main>
{body}
    <footer>{repo_line}</footer>
  </main>
</div>
<a class="totop" href="#" aria-label="Back to top">&uarr; Top</a>
<script>{JS}</script>
</body>
</html>
""", encoding="utf-8")

    kb = args.output.stat().st_size / 1024
    print(f"wrote {args.output} ({kb:.0f} KB, {toc.count('<a ')} nav entries, "
          f"{n_sources} linked sources)")
    if n_sources == 0:
        print("  note: no bibliography <ol> found, citations will not be linked",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
