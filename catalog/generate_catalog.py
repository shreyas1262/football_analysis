#!/usr/bin/env python3
"""Regenerate the FBref silver-layer data catalog (data_catalog.html) from the live
DuckDB warehouse.

Run it from anywhere:
    python catalog/generate_catalog.py

It reads column metadata + row counts straight from data/warehouse.duckdb (built by
`dbt build`), so the catalog always reflects the current silver schema. The ER diagram
(er_diagram.html) is hand-maintained and not touched by this script.
"""
import os
import pathlib
import html as html_mod
import duckdb

HERE = pathlib.Path(__file__).resolve().parent          # .../football_analysis/catalog
REPO = HERE.parent                                       # .../football_analysis
# The silver views embed delta_scan('../data/bronze/...') relative to the dbt dir,
# so query from there for the paths to resolve.
os.chdir(REPO / "dbt")

con = duckdb.connect("../data/warehouse.duckdb")
con.execute("LOAD delta;")

# ---- pull schema + counts ----
rows = con.execute("""
  select table_name, column_name, data_type, ordinal_position
  from information_schema.columns
  where table_schema='main' and table_name like 'stg_fbref_%'
  order by table_name, ordinal_position
""").df()
schema = {}
for _, r in rows.iterrows():
    schema.setdefault(r['table_name'], []).append((r['column_name'], r['data_type']))
counts = {m: con.execute(f"select count(*) from main.{m}").fetchone()[0] for m in schema}


def T(short, grain, pk, fk=None, family="team", note=""):
    return dict(short=short, grain=grain, pk=pk, fk=fk, family=family, note=note)


META = {
 "stg_fbref_seasons": T("seasons", "one row per league-season", "league + season", family="dim",
                        note="Dimension: which league-seasons exist."),
 "stg_fbref_schedule": T("schedule", "one row per match", "match_id", family="match-anchor",
                        note="The league fixture list — anchor for all match-grain tables."),
 "stg_fbref_player_season_stats_standard": T("standard", "player · team · season", "player_id", family="player"),
 "stg_fbref_player_season_stats_shooting": T("shooting", "player · team · season", "player_id", family="player"),
 "stg_fbref_player_season_stats_keeper":   T("keeper", "player · team · season", "player_id", family="player",
                        note="Goalkeepers only."),
 "stg_fbref_player_season_stats_playing_time": T("playing_time", "player · team · season", "player_id", family="player"),
 "stg_fbref_player_season_stats_misc":     T("misc", "player · team · season", "player_id", family="player"),
 "stg_fbref_team_season_stats_standard": T("standard", "team · season", "team_id + season", family="team-season"),
 "stg_fbref_team_season_stats_shooting": T("shooting", "team · season", "team_id + season", family="team-season"),
 "stg_fbref_team_season_stats_keeper":   T("keeper", "team · season", "team_id + season", family="team-season"),
 "stg_fbref_team_season_stats_playing_time": T("playing_time", "team · season", "team_id + season", family="team-season"),
 "stg_fbref_team_season_stats_misc":     T("misc", "team · season", "team_id + season", family="team-season"),
 "stg_fbref_team_match_stats_schedule": T("schedule", "team · match", "team_match_id", "match_id", family="team-match",
                        note="Match log with tactics: formation, possession, captain."),
 "stg_fbref_team_match_stats_shooting": T("shooting", "team · match", "team_match_id", "match_id", family="team-match"),
 "stg_fbref_team_match_stats_keeper":   T("keeper", "team · match", "team_match_id", "match_id", family="team-match"),
 "stg_fbref_team_match_stats_misc":     T("misc", "team · match", "team_match_id", "match_id", family="team-match"),
}

FAMILY_LABEL = {
 "dim": "Dimension", "match-anchor": "Schedule", "player": "Player-Season",
 "team-season": "Team-Season", "team-match": "Team-Match",
}


def typebadge(t):
    return {"VARCHAR": "str", "INTEGER": "int", "BIGINT": "int", "DOUBLE": "num", "FLOAT": "num",
            "DATE": "date", "BOOLEAN": "bool", "TIMESTAMP": "ts"}.get(t, t.lower())


def keyrole(model, col):
    m = META[model]
    pkcols = [c.strip() for c in m["pk"].replace("+", ",").split(",")]
    if col == m.get("fk"):
        return "fk"
    if col in pkcols:
        return "pk"
    return None


def esc(s):
    return html_mod.escape(str(s))


ORDER = ["dim", "match-anchor", "player", "team-season", "team-match"]
groups = {k: [] for k in ORDER}
for model, meta in META.items():
    groups[meta["family"]].append(model)


def render_card(model):
    meta = META[model]
    cols = schema[model]
    n = counts[model]
    fam = meta["family"]
    chips = []
    for col, dt in cols:
        role = keyrole(model, col)
        cls = f" col--{role}" if role else ""
        badge = f'<span class="krole {role}">{role.upper()}</span>' if role else ""
        chips.append(
            f'<li class="col{cls}"><code>{esc(col)}</code>'
            f'<span class="ctype">{esc(typebadge(dt))}</span>{badge}</li>')
    note = f'<p class="note">{esc(meta["note"])}</p>' if meta["note"] else ""
    fk = f'<span class="tag fk">FK&nbsp;{esc(meta["fk"])}&nbsp;→ schedule</span>' if meta.get("fk") else ""
    return f"""
    <article class="card fam-{fam}">
      <header class="card-h">
        <div class="card-title">
          <span class="fam-dot"></span>
          <h3>{esc(meta['short'])}</h3>
          <span class="model-id">{esc(model)}</span>
        </div>
        <span class="rowcount">{n:,}<small>rows</small></span>
      </header>
      <div class="card-meta">
        <span class="tag grain">{esc(meta['grain'])}</span>
        <span class="tag pk">PK&nbsp;{esc(meta['pk'])}</span>
        {fk}
      </div>
      {note}
      <ul class="cols">{''.join(chips)}</ul>
    </article>"""


sections = []
for fam in ORDER:
    models = groups[fam]
    if not models:
        continue
    total = sum(counts[m] for m in models)
    cards = "\n".join(render_card(m) for m in models)
    sections.append(f"""
    <section class="fam-section" data-fam="{fam}">
      <div class="fam-head">
        <h2>{FAMILY_LABEL[fam]}</h2>
        <span class="fam-count">{len(models)} model{'s' if len(models) > 1 else ''} · {total:,} rows</span>
      </div>
      <div class="grid">{cards}</div>
    </section>""")

body_cards = "\n".join(sections)
total_models = len(META)
total_rows = sum(counts.values())

CSS = r"""
:root{
  --ground:#f3f6f2; --surface:#ffffff; --surface-2:#eaf0ea; --raise:#ffffff;
  --ink:#15211b; --ink-2:#3f4f47; --ink-3:#6b7c72; --line:#dbe4dd; --line-2:#c9d5cc;
  --accent:#0f7a43; --accent-ink:#0a5c32;
  --fam-dim:#3268a6; --fam-match-anchor:#0f7a43; --fam-player:#a8720c;
  --fam-team-season:#6a52c9; --fam-team-match:#0d8a80;
  --pk:#0f7a43; --fk:#a8720c;
  --shadow:0 1px 2px rgba(20,40,30,.06),0 6px 20px rgba(20,40,30,.06);
  --mono:ui-monospace,"SF Mono",Menlo,Consolas,"Liberation Mono",monospace;
  --sans:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --ground:#0c1310; --surface:#121c17; --surface-2:#182420; --raise:#16221c;
  --ink:#e9f1eb; --ink-2:#a7b8ad; --ink-3:#7a8c82; --line:#24322b; --line-2:#2f3f37;
  --accent:#37c07b; --accent-ink:#8fe3b6;
  --fam-dim:#6fa3dc; --fam-match-anchor:#37c07b; --fam-player:#dca63f;
  --fam-team-season:#a493f0; --fam-team-match:#3fc3b8;
  --pk:#37c07b; --fk:#dca63f;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 8px 24px rgba(0,0,0,.28);
}}
:root[data-theme="dark"]{
  --ground:#0c1310; --surface:#121c17; --surface-2:#182420; --raise:#16221c;
  --ink:#e9f1eb; --ink-2:#a7b8ad; --ink-3:#7a8c82; --line:#24322b; --line-2:#2f3f37;
  --accent:#37c07b; --accent-ink:#8fe3b6;
  --fam-dim:#6fa3dc; --fam-match-anchor:#37c07b; --fam-player:#dca63f;
  --fam-team-season:#a493f0; --fam-team-match:#3fc3b8;
  --pk:#37c07b; --fk:#dca63f;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 8px 24px rgba(0,0,0,.28);
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--ground);color:var(--ink);font-family:var(--sans);
  line-height:1.55;font-size:16px;-webkit-font-smoothing:antialiased;}
.wrap{max-width:1120px;margin:0 auto;padding:0 24px 96px;}
a{color:var(--accent-ink)}
.masthead{padding:56px 0 28px;border-bottom:1px solid var(--line);}
.eyebrow{font-family:var(--mono);font-size:12px;letter-spacing:.16em;text-transform:uppercase;
  color:var(--accent-ink);margin:0 0 14px;display:flex;align-items:center;gap:10px;}
.eyebrow::before{content:"";width:22px;height:2px;background:var(--accent);border-radius:2px;}
h1{font-size:clamp(30px,5vw,46px);line-height:1.04;letter-spacing:-.02em;margin:0 0 12px;
  font-weight:680;text-wrap:balance;}
.lede{font-size:18px;color:var(--ink-2);max-width:62ch;margin:0;}
.stats{display:flex;flex-wrap:wrap;gap:10px 12px;margin-top:26px;}
.stat{background:var(--surface);border:1px solid var(--line);border-radius:11px;padding:12px 16px;
  box-shadow:var(--shadow);min-width:104px;}
.stat b{display:block;font-size:24px;font-weight:680;letter-spacing:-.01em;
  font-variant-numeric:tabular-nums;line-height:1.1;}
.stat span{font-size:12px;color:var(--ink-3);font-family:var(--mono);letter-spacing:.04em;
  text-transform:uppercase;}
h2.block{font-size:13px;font-family:var(--mono);letter-spacing:.14em;text-transform:uppercase;
  color:var(--ink-3);margin:56px 0 18px;font-weight:600;}
figure.diagram{margin:0;background:var(--surface);border:1px solid var(--line);border-radius:16px;
  padding:26px 24px 18px;box-shadow:var(--shadow);overflow:hidden;}
.diagram svg{display:block;width:100%;height:auto;color:var(--ink-2);}
.diagram figcaption{margin-top:14px;font-size:13.5px;color:var(--ink-2);max-width:80ch;}
.diagram figcaption b{color:var(--ink);font-weight:620;}
.d-node{fill:var(--surface-2);stroke:var(--line-2);stroke-width:1.2;}
.d-lbl{fill:var(--ink);font-family:var(--mono);font-size:12.5px;}
.d-sub{fill:var(--ink-3);font-family:var(--mono);font-size:10.5px;}
.d-thread{fill:none;stroke-width:2.2;}
.d-keytag{font-family:var(--mono);font-size:10.5px;font-weight:600;}
.t-green{stroke:var(--fam-match-anchor)} .f-green{fill:var(--fam-match-anchor)}
.t-teal{stroke:var(--fam-team-match)}   .box-teal{stroke:var(--fam-team-match);fill:var(--surface-2)}
.t-amber{stroke:var(--fam-player)}      .box-amber{stroke:var(--fam-player);fill:var(--surface-2)}
.t-violet{stroke:var(--fam-team-season)} .box-violet{stroke:var(--fam-team-season);fill:var(--surface-2)}
.pill-green{fill:var(--fam-match-anchor)} .pill-amber{fill:var(--fam-player)} .pill-violet{fill:var(--fam-team-season)}
.pill-lbl{fill:#fff;font-family:var(--mono);font-size:12.5px;font-weight:600;}
.pill-sub{fill:rgba(255,255,255,.82);font-family:var(--mono);font-size:10px;}
.legend{display:flex;flex-wrap:wrap;gap:8px 18px;margin:16px 2px 0;font-size:12.5px;color:var(--ink-2);}
.legend span{display:inline-flex;align-items:center;gap:7px;font-family:var(--mono);}
.swatch{width:11px;height:11px;border-radius:3px;display:inline-block;}
.kbadge{font-family:var(--mono);font-size:10px;font-weight:700;padding:1px 5px;border-radius:4px;
  letter-spacing:.03em;}
.kbadge.pk{color:#fff;background:var(--pk)} .kbadge.fk{color:#fff;background:var(--fk)}
.fam-section{margin-top:12px;}
.fam-head{display:flex;align-items:baseline;gap:14px;margin:38px 0 16px;padding-bottom:10px;
  border-bottom:1px solid var(--line);}
.fam-head h2{font-size:19px;margin:0;letter-spacing:-.01em;font-weight:660;}
.fam-count{font-family:var(--mono);font-size:12px;color:var(--ink-3);letter-spacing:.03em;}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;}
.card{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:16px 16px 6px;
  box-shadow:var(--shadow);border-top:3px solid var(--fam);position:relative;display:flex;flex-direction:column;}
.fam-dim{--fam:var(--fam-dim)} .fam-match-anchor{--fam:var(--fam-match-anchor)}
.fam-player{--fam:var(--fam-player)} .fam-team-season{--fam:var(--fam-team-season)}
.fam-team-match{--fam:var(--fam-team-match)}
.card-h{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;}
.card-title{display:flex;align-items:center;gap:9px;flex-wrap:wrap;}
.fam-dot{width:9px;height:9px;border-radius:50%;background:var(--fam);flex:none;}
.card-title h3{margin:0;font-size:17px;font-weight:660;letter-spacing:-.01em;}
.model-id{font-family:var(--mono);font-size:11px;color:var(--ink-3);}
.rowcount{font-family:var(--mono);font-weight:680;font-size:17px;text-align:right;line-height:1;
  font-variant-numeric:tabular-nums;flex:none;}
.rowcount small{display:block;font-size:9.5px;font-weight:500;color:var(--ink-3);letter-spacing:.08em;
  text-transform:uppercase;margin-top:3px;}
.card-meta{display:flex;flex-wrap:wrap;gap:6px;margin:12px 0 2px;}
.tag{font-family:var(--mono);font-size:11px;padding:3px 8px;border-radius:6px;background:var(--surface-2);
  color:var(--ink-2);border:1px solid var(--line);}
.tag.grain{color:var(--ink)}
.tag.pk{color:var(--pk);border-color:color-mix(in srgb,var(--pk) 32%,var(--line))}
.tag.fk{color:var(--fk);border-color:color-mix(in srgb,var(--fk) 32%,var(--line))}
.note{font-size:13px;color:var(--ink-2);margin:10px 0 2px;}
.cols{list-style:none;margin:12px 0 10px;padding:12px 0 0;border-top:1px dashed var(--line-2);
  display:flex;flex-direction:column;gap:1px;}
.col{display:flex;align-items:center;gap:8px;padding:3px 4px;border-radius:5px;}
.col code{font-family:var(--mono);font-size:12.5px;color:var(--ink);}
.col .ctype{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);margin-left:auto;
  padding:1px 6px;background:var(--surface-2);border-radius:4px;letter-spacing:.02em;}
.krole{font-family:var(--mono);font-size:9px;font-weight:700;padding:1px 5px;border-radius:4px;color:#fff;}
.krole.pk{background:var(--pk)} .krole.fk{background:var(--fk)}
.col--pk code,.col--fk code{font-weight:640;}
.col--pk .ctype{order:2} .col--fk .ctype{order:2}
.callouts{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px;}
.callout{background:var(--surface);border:1px solid var(--line);border-left:3px solid var(--accent);
  border-radius:12px;padding:16px 18px;box-shadow:var(--shadow);}
.callout.warn{border-left-color:var(--fk)}
.callout h3{margin:0 0 8px;font-size:15px;font-weight:640;display:flex;align-items:center;gap:8px;}
.callout p{margin:0 0 8px;font-size:13.5px;color:var(--ink-2);}
.callout p:last-child{margin-bottom:0}
.callout code{font-family:var(--mono);font-size:12px;background:var(--surface-2);padding:1px 5px;
  border-radius:4px;color:var(--ink);}
.tick{color:var(--accent-ink)} .flag{color:var(--fk)}
.nametbl-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px;background:var(--surface);
  box-shadow:var(--shadow);}
table.nametbl{border-collapse:collapse;width:100%;min-width:560px;font-size:13px;}
.nametbl th,.nametbl td{text-align:left;padding:11px 16px;border-bottom:1px solid var(--line);}
.nametbl th{font-family:var(--mono);font-size:11px;letter-spacing:.06em;text-transform:uppercase;
  color:var(--ink-3);font-weight:600;background:var(--surface-2);}
.nametbl tr:last-child td{border-bottom:none}
.nametbl code{font-family:var(--mono);font-size:12px;color:var(--ink);}
.nametbl .c1{color:var(--fam-player)} .nametbl .c2{color:var(--fam-team-season)}
footer{margin-top:60px;padding-top:22px;border-top:1px solid var(--line);font-family:var(--mono);
  font-size:12px;color:var(--ink-3);display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;}
"""


def column(cx, pill_cls, pill_l, pill_sub, thread_cls, box_cls, boxes):
    top = 28
    pw = 224
    x = cx - pw / 2
    parts = []
    last_cy = 92 + (len(boxes) - 1) * 62 + 23
    parts.append(f'<line class="d-thread {thread_cls}" x1="{cx}" y1="{top+34}" x2="{cx}" y2="{last_cy}"/>')
    parts.append(f'<rect class="{pill_cls}" x="{x}" y="{top}" width="{pw}" height="34" rx="8"/>')
    parts.append(f'<text class="pill-lbl" x="{cx}" y="{top+15}" text-anchor="middle">{pill_l}</text>')
    parts.append(f'<text class="pill-sub" x="{cx}" y="{top+28}" text-anchor="middle">{pill_sub}</text>')
    for i, b in enumerate(boxes):
        by = 92 + i * 62
        parts.append(f'<rect class="d-node {box_cls}" x="{x}" y="{by}" width="{pw}" height="46" rx="7"/>')
        parts.append(f'<text class="d-lbl" x="{cx}" y="{by+21}" text-anchor="middle">{b[0]}</text>')
        parts.append(f'<text class="d-sub" x="{cx}" y="{by+36}" text-anchor="middle">{b[1]}</text>')
    return "\n".join(parts)


SVG = f"""<svg viewBox="0 0 924 470" role="img" aria-label="The silver layer is three table-families joined by three keys, all anchored to the league schedule.">
  <text class="d-keytag f-green" x="148" y="18" text-anchor="middle">match_id (FK &rarr; schedule)</text>
  {column(148, "pill-green", "stg_fbref_schedule", "anchor · PK match_id", "t-green", "box-teal",
     [("match · schedule", "team_match_id · tactics"), ("match · shooting", "team_match_id"),
      ("match · keeper", "team_match_id"), ("match · misc", "team_match_id")])}
  {column(462, "pill-amber", "player_id", "surrogate: player+born+nation", "t-amber", "box-amber",
     [("player · standard", "goals, assists, cards"), ("player · shooting", "shots, rates"),
      ("player · keeper", "GK only — 212 rows"), ("player · playing_time", "minutes, subs, on-pitch"),
      ("player · misc", "fouls, tackles, offsides")])}
  {column(776, "pill-violet", "team_id + season", "composite PK", "t-violet", "box-violet",
     [("team · standard", "goals, poss, per-90"), ("team · shooting", "shots, conversion"),
      ("team · keeper", "GK aggregates"), ("team · playing_time", "squad usage, PPM"),
      ("team · misc", "fouls, tackles, offsides")])}
</svg>"""

LEGEND = """
<div class="legend">
  <span><i class="swatch" style="background:var(--fam-match-anchor)"></i>Schedule (anchor)</span>
  <span><i class="swatch" style="background:var(--fam-team-match)"></i>Team-Match</span>
  <span><i class="swatch" style="background:var(--fam-player)"></i>Player-Season</span>
  <span><i class="swatch" style="background:var(--fam-team-season)"></i>Team-Season</span>
  <span><i class="swatch" style="background:var(--fam-dim)"></i>Dimension</span>
  <span><i class="kbadge pk">PK</i>primary key</span>
  <span><i class="kbadge fk">FK</i>foreign key</span>
</div>"""

DIAGRAM = f"""
<h2 class="block">How it joins</h2>
<figure class="diagram">
  {SVG}
  <figcaption><b>Three families, three keys.</b> Match-grain tables share <code>match_id</code> and
  all trace back to <code>stg_fbref_schedule</code> (the authoritative league fixture list). Player tables
  join on the <code>player_id</code> surrogate; team-season tables on the <code>(team_id, season)</code>
  composite. Join within a family on its key; bridge match&harr;season grains through schedule.</figcaption>
</figure>
{LEGEND}"""

QUALITY = """
<h2 class="block">Data quality &amp; decisions</h2>
<div class="callouts">
  <div class="callout"><h3><span class="tick">&#10003;</span> League-only, gated on schedule</h3>
    <p>Team-match tables keep a row only if its <code>match_id</code> exists in
    <code>stg_fbref_schedule</code> &mdash; the reliable league-fixture list.</p>
    <p>This recovered <b>310 league matches</b> a naive <code>league != 'nan'</code> filter had dropped:
    the bronze <code>league</code> column is unreliably <code>'nan'</code> even for real league games.</p></div>
  <div class="callout warn"><h3><span class="flag">&#9873;</span> NaN is a string, not NULL</h3>
    <p>Bronze stringified pandas <code>NaN</code>, so nulls arrive as the literal text <code>'nan'</code>.
    Filter with <code>!= 'nan'</code>, never <code>IS NOT NULL</code>.</p></div>
  <div class="callout warn"><h3><span class="flag">&#9873;</span> Dates from the game string</h3>
    <p>The bronze <code>date</code> is a tz-aware timestamp; <code>date::date</code> is off by one day.
    Match dates come from <code>split_part(game,' ',1)</code> instead.</p></div>
  <div class="callout"><h3><span class="tick">&#10003;</span> Cup data intentionally excluded</h3>
    <p>FBref only tracks Big-5 teams' cup games (often one-sided, with <code>nan</code> stats), so cup
    matches are left in bronze. Silver is strictly the Big-5 leagues.</p></div>
</div>"""

NAMING = """
<h2 class="block">Naming &mdash; reconciled <span style="color:var(--accent-ink)">&#10003;</span></h2>
<p style="color:var(--ink-2);font-size:14px;max-width:74ch;margin:0 0 16px;">Building the catalog surfaced
drift between the player and team models &mdash; the same concept spelled differently across tables. It's
now aligned to one vocabulary, so unions and joins in gold speak the same language. The canonical form and
what was folded into it:</p>
<div class="nametbl-wrap"><table class="nametbl">
  <thead><tr><th>Concept</th><th>Canonical</th><th>Folded in from</th></tr></thead>
  <tbody>
    <tr><td>Save percentage</td><td class="c2"><code>save_pct</code></td><td class="c1"><code>save_percentage</code></td></tr>
    <tr><td>Clean-sheet %</td><td class="c2"><code>clean_sheet_pct</code></td><td class="c1"><code>clean_sheet_percentage</code></td></tr>
    <tr><td>Pen. save %</td><td class="c2"><code>penalty_save_pct</code></td><td class="c1"><code>penalty_kick_save_percentage</code></td></tr>
    <tr><td>Pens. scored against keeper</td><td class="c2"><code>penalties_allowed</code></td><td class="c1"><code>penalties_scored_against</code></td></tr>
    <tr><td>Pens. missed by shooter</td><td class="c2"><code>penalties_missed</code></td><td class="c1"><code>penalties_missed_by_shooter</code></td></tr>
    <tr><td>Player name</td><td class="c2"><code>name</code></td><td class="c1"><code>player_name</code> (shooting)</td></tr>
    <tr><td>Shots per 90</td><td class="c2"><code>shots_per_90</code></td><td class="c1"><code>shots_per_90_min</code></td></tr>
    <tr><td>Shots on target per 90</td><td class="c2"><code>shots_on_target_per_90</code></td><td class="c1"><code>shots_on_target_per_90_min</code></td></tr>
    <tr><td>Pen. attempts</td><td class="c2"><code>penalties_attempted</code></td><td class="c1"><code>penalty_attempts</code></td></tr>
    <tr><td>90-min periods</td><td class="c2"><code>minutes_90s</code></td><td class="c1"><code>no_of_90_min_periods_played</code>, <code>minutes_played_per_90</code></td></tr>
  </tbody>
</table></div>"""

HTML = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FBref Silver Layer — Data Catalog</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
  <header class="masthead">
    <p class="eyebrow">Football Analytics &middot; Silver Layer</p>
    <h1>FBref Data Catalog</h1>
    <p class="lede">Every cleaned model in the DuckDB silver layer &mdash; grain, keys, columns and row
    counts &mdash; built from Delta bronze via dbt. The map to build gold from.</p>
    <div class="stats">
      <div class="stat"><b>{total_models}</b><span>models</span></div>
      <div class="stat"><b>{total_rows:,}</b><span>rows</span></div>
      <div class="stat"><b>5</b><span>Big-5 leagues</span></div>
      <div class="stat"><b>2024&ndash;25</b><span>season</span></div>
      <div class="stat"><b>3</b><span>join keys</span></div>
    </div>
  </header>
  {DIAGRAM}
  <h2 class="block">The models</h2>
  {body_cards}
  {QUALITY}
  {NAMING}
  <footer>
    <span>Silver layer &middot; dbt-duckdb over Delta bronze</span>
    <span>grain: player-season &middot; team-season &middot; team-match</span>
  </footer>
</div>
</body>
</html>"""

out = HERE / "data_catalog.html"
out.write_text(HTML, encoding="utf-8")
print(f"wrote {out}")
print(f"  {total_models} models, {total_rows:,} rows")
