#!/usr/bin/env python3
"""Generate per-participant HTML reports from structured survey data."""
from __future__ import annotations

import argparse
import html
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import folium
import pandas as pd
from markdown import markdown


CONSENT_FIELDS = {
    "submitted_at": "Consent submitted on",
    "consent_followup_contact": "Participant consented to follow-up contact",
}

INITIAL_QUESTION_MAP = {
    "age": "How old are you?",
    "gender": "Which gender do you identify with?",
    "sexuality": "How do you describe your sexual orientation?",
    "ethnicity": "Which ethnicities do you identify with?",
    "birth_place": "Where were you born?",
    "lives_in_barcelona": "Do you currently live in Barcelona?",
    "suburb": "Which suburb do you live in?",
    "building_type": "What kind of building do you live in?",
    "household_items": "Which of the following items are in your household?",
    "education": "What is the highest level of education you have completed?",
    "climate_activism": "Have you taken part in any climate activism?",
    "general_health": "How would you rate your general health?",
    "activities": "Which activities are part of your day-to-day life?",
    "living_arrangement": "Which description best matches your current living arrangement?",
    "relationship_status": "What is your current relationship status?",
    "cheerful_spirits": "I have felt cheerful and in good spirits.",
    "calm_relaxed": "I have felt calm and relaxed.",
    "active_vigorous": "I have felt active and vigorous.",
    "woke_up_fresh": "I woke up feeling fresh and rested.",
    "daily_life_interesting": "My daily life has been filled with things that interest me.",
    "cooperate_with_people": "I feel I can cooperate well with other people.",
    "improving_skills": "I feel that I am improving my skills or learning new things.",
    "social_situations": "I feel comfortable in social situations.",
    "family_support": "I feel that my family supports me.",
    "family_knows_me": "I feel that my family knows me well.",
    "access_to_food": "I have access to enough food.",
    "people_enjoy_time": "People enjoy spending time with me.",
    "talk_to_family": "I am able to talk to my family when I need to.",
    "friends_support": "I feel that my friends support me.",
    "belong_in_community": "I feel that I belong in my community.",
    "family_stands_by_me": "My family stands by me in difficult times.",
    "friends_stand_by_me": "My friends stand by me in difficult times.",
    "treated_fairly": "I feel I am treated fairly by people around me.",
    "opportunities_responsibility": "I have opportunities to take on responsibility.",
    "secure_with_family": "I feel secure when I am with my family.",
    "opportunities_abilities": "I have the opportunity to use my abilities.",
    "enjoy_cultural_traditions": "I have been able to enjoy my cultural traditions.",
    "environmental_challenges": "I am aware of environmental challenges in my area.",
    "challenges_stress_level": "Thinking about challenges I face raises my stress levels.",
    "coping_help": "I know where to find help when I am struggling to cope.",
}

BIWEEKLY_QUESTION_MAP = {
    "activities": "Which activities best describe your life right now?",
    "living_arrangement": "Which description best matches your current living arrangement?",
    "relationship_status": "What is your current relationship status?",
    "cheerful_spirits": "I have felt cheerful and in good spirits.",
    "calm_relaxed": "I have felt calm and relaxed.",
    "active_vigorous": "I have felt active and vigorous.",
    "woke_up_fresh": "I woke up feeling fresh and rested.",
    "daily_life_interesting": "My daily life has been filled with things that interest me.",
    "cooperate_with_people": "I feel I can cooperate well with other people.",
    "improving_skills": "I feel that I am improving my skills or learning new things.",
    "social_situations": "I feel comfortable in social situations.",
    "family_support": "I feel that my family supports me.",
    "family_knows_me": "I feel that my family knows me well.",
    "access_to_food": "I have access to enough food.",
    "people_enjoy_time": "People enjoy spending time with me.",
    "talk_to_family": "I am able to talk to my family when I need to.",
    "friends_support": "I feel that my friends support me.",
    "belong_in_community": "I feel that I belong in my community.",
    "family_stands_by_me": "My family stands by me in difficult times.",
    "friends_stand_by_me": "My friends stand by me in difficult times.",
    "treated_fairly": "I feel I am treated fairly by people around me.",
    "opportunities_responsibility": "I have opportunities to take on responsibility.",
    "secure_with_family": "I feel secure when I am with my family.",
    "opportunities_abilities": "I have the opportunity to use my abilities.",
    "enjoy_cultural_traditions": "I have been able to enjoy my cultural traditions.",
    "environmental_challenges": "I am aware of environmental challenges in my area.",
    "challenges_stress_level": "Thinking about challenges I face raises my stress levels.",
    "coping_help": "I know where to find help when I am struggling to cope.",
}


@dataclass
class ParticipantData:
    participant_uuid: str
    consent: pd.Series
    initial: Optional[pd.Series]
    biweekly: List[pd.Series]
    signature: Optional[str]
    consent_submitted_at: Optional[pd.Timestamp]
    initial_submitted_at: Optional[pd.Timestamp]
    latest_biweekly_at: Optional[pd.Timestamp]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate per-participant reports from structured survey CSVs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", required=True, help="Directory containing structured CSV files.")
    parser.add_argument(
        "--output",
        required=True,
        help="Directory where participant reports should be written.",
    )
    parser.add_argument(
        "--maps-subdir",
        default="maps",
        help="Relative directory (inside output) to store interactive map files.",
    )
    parser.add_argument(
        "--question-config",
        help="Optional JSON file providing custom question text overrides.",
    )
    return parser.parse_args()


def load_question_overrides(path: Optional[str]) -> Dict[str, Dict[str, str]]:
    if not path:
        return {}
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Question config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return {
        "initial": data.get("initial", {}),
        "biweekly": data.get("biweekly", {}),
        "consent": data.get("consent", {}),
    }


def safe_filename(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in name)


def is_blank(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in {"nan", "none"}
    return False


def format_boolean(value) -> str:
    if is_blank(value):
        return "No response recorded"
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return "Yes"
    if text in {"0", "false", "no"}:
        return "No"
    return str(value)


def parse_multi(value) -> List[str]:
    if is_blank(value):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if not is_blank(item)]
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            return [str(item) for item in parsed if not is_blank(item)]
        except (json.JSONDecodeError, TypeError):
            pass
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def format_answer(value) -> str:
    if is_blank(value):
        return "No response recorded"
    items = parse_multi(value)
    if len(items) > 1:
        return ", ".join(items)
    if len(items) == 1:
        return items[0]
    return str(value)


def parse_timestamp(value) -> Optional[pd.Timestamp]:
    if is_blank(value):
        return None
    try:
        ts = pd.to_datetime(value)
    except (ValueError, TypeError):
        return None
    if isinstance(ts, pd.Timestamp):
        return ts
    try:
        ts = ts.iloc[0]
    except Exception:  # noqa: BLE001
        return None
    return ts if isinstance(ts, pd.Timestamp) else None


def extract_timestamp(row: Optional[pd.Series]) -> Optional[pd.Timestamp]:
    if row is None:
        return None
    for column in ("submitted_at", "timestamp", "created_at"):
        if column in row and not is_blank(row[column]):
            ts = parse_timestamp(row[column])
            if ts is not None:
                return ts
    return None


def format_timestamp(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return "n/a"
    try:
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
    except AttributeError:
        pass
    except Exception:  # noqa: BLE001
        return ts.strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%d")


def format_question_block(row: pd.Series, question_map: Dict[str, str]) -> str:
    lines: List[str] = []
    for column, question in question_map.items():
        if column in row and not is_blank(row[column]):
            answer = format_answer(row[column])
            lines.append(f"**{question}**\n\n{answer}")
    if not lines:
        return "_No responses recorded._"
    return "\n\n".join(lines)


def _choose_latest(df: pd.DataFrame) -> pd.Series:
    for column in ("submitted_at", "timestamp", "created_at"):
        if column in df.columns:
            try:
                return df.sort_values(column).iloc[-1]
            except Exception:  # noqa: BLE001
                continue
    return df.iloc[-1]


def build_participant_data(
    consent_df: pd.DataFrame,
    initial_df: pd.DataFrame,
    biweekly_df: pd.DataFrame,
) -> List[ParticipantData]:
    participants: List[ParticipantData] = []
    if "participant_uuid" not in consent_df.columns:
        return participants

    for participant_uuid, consent_records in consent_df.groupby("participant_uuid"):
        participant_uuid_str = str(participant_uuid or "").strip()
        if not participant_uuid_str:
            continue

        consent_latest = _choose_latest(consent_records)
        consent_ts = extract_timestamp(consent_latest)

        initial_record = None
        initial_ts: Optional[pd.Timestamp] = None
        if not initial_df.empty and "participant_uuid" in initial_df.columns:
            initial_matches = initial_df[initial_df["participant_uuid"] == participant_uuid]
            if not initial_matches.empty:
                initial_record = _choose_latest(initial_matches)
                initial_ts = extract_timestamp(initial_record)

        biweekly_rows: List[pd.Series] = []
        latest_biweekly_ts: Optional[pd.Timestamp] = None
        if not biweekly_df.empty and "participant_uuid" in biweekly_df.columns:
            biweekly_matches = biweekly_df[biweekly_df["participant_uuid"] == participant_uuid]
            if not biweekly_matches.empty:
                if "submitted_at" in biweekly_matches.columns:
                    biweekly_matches = biweekly_matches.sort_values("submitted_at")
                biweekly_rows = [row for _, row in biweekly_matches.iterrows()]
                for row in biweekly_rows:
                    ts = extract_timestamp(row)
                    if ts is not None and (latest_biweekly_ts is None or ts > latest_biweekly_ts):
                        latest_biweekly_ts = ts

        participants.append(
            ParticipantData(
                participant_uuid=participant_uuid_str,
                consent=consent_latest,
                initial=initial_record,
                biweekly=biweekly_rows,
                signature=str(consent_latest.get("participant_signature", "")).strip() or None,
                consent_submitted_at=consent_ts,
                initial_submitted_at=initial_ts,
                latest_biweekly_at=latest_biweekly_ts,
            )
        )
    return participants


def create_map_for_response(
    response_id: str,
    locations: pd.DataFrame,
    maps_dir: Path,
    maps_subdir: str,
) -> Optional[str]:
    response_locations = locations[locations["response_id"] == response_id]
    if response_locations.empty:
        return None

    coords: List[Tuple[float, float, float]] = []
    for _, loc in response_locations.iterrows():
        try:
            lat = float(loc.get("latitude"))
            lon = float(loc.get("longitude"))
            accuracy = float(loc.get("accuracy", 0) or 0)
        except (TypeError, ValueError):
            continue
        coords.append((lat, lon, accuracy))

    if not coords:
        return None

    first_lat, first_lon, _ = coords[0]
    fmap = folium.Map(location=[first_lat, first_lon], zoom_start=13)
    for (lat, lon, accuracy), (_, loc) in zip(coords, response_locations.iterrows()):
        tooltip = loc.get("timestamp") or "Location sample"
        radius = max(accuracy, 5.0)
        folium.Circle(
            location=(lat, lon),
            radius=radius,
            color="#1d4ed8",
            weight=1,
            fill=True,
            fill_color="#1d4ed8",
            fill_opacity=0.35,
            tooltip=tooltip,
        ).add_to(fmap)

    maps_dir.mkdir(parents=True, exist_ok=True)
    filename = safe_filename(f"{response_id}_map") + ".html"
    fmap.save(maps_dir / filename)
    relative_path = f"{maps_subdir}/{filename}"
    iframe = (
        f'<iframe src="{relative_path}" width="100%" height="400" '
        "style='border:1px solid #d1d5db; border-radius:8px; margin-top:0.5rem;'></iframe>"
    )
    return iframe

def render_consent_section(consent: pd.Series, signature: Optional[str]) -> str:
    submitted_at = consent.get("submitted_at") or consent.get("timestamp") or "Unknown"
    contact = format_boolean(consent.get("consent_followup_contact"))
    participant_code = signature or "Not provided"
    lines = [
        "## Consent summary",
        "",
        f"- **Participant code:** {participant_code}",
        f"- **Consent submitted:** {submitted_at}",
        f"- **Agreed to follow-up contact:** {contact}",
    ]
    return "\n".join(lines)


def render_initial_section(initial: Optional[pd.Series], override: Dict[str, str]) -> str:
    if initial is None:
        return "## Initial survey\n\n_No initial survey submission found._"
    question_map = {**INITIAL_QUESTION_MAP, **override}
    content = format_question_block(initial, question_map)
    submitted_at = initial.get("submitted_at") or initial.get("timestamp") or "Unknown"
    lines = [
        "## Initial survey",
        "",
        f"_Submitted at {submitted_at}_",
        "",
        content,
    ]
    return "\n".join(line for line in lines if line is not None)


def render_biweekly_section(
    biweekly_rows: List[pd.Series],
    override: Dict[str, str],
    locations: pd.DataFrame,
    maps_dir: Path,
    maps_subdir: str,
) -> str:
    if not biweekly_rows:
        return "## Biweekly surveys\n\n_No biweekly submissions recorded._"

    question_map = {**BIWEEKLY_QUESTION_MAP, **override}
    sections: List[str] = ["## Biweekly surveys"]

    for row in biweekly_rows:
        submitted_at = row.get("submitted_at") or row.get("timestamp") or "Unknown"
        response_id = str(row.get("response_id", "unknown"))
        content = format_question_block(row, question_map)
        section_lines = [
            f"### Biweekly survey – {submitted_at}",
            "",
            content,
        ]
        section_md = "\n".join(section_lines)
        map_iframe = create_map_for_response(
            response_id=response_id,
            locations=locations,
            maps_dir=maps_dir,
            maps_subdir=maps_subdir,
        )
        if map_iframe:
            section_md += "\n\n" + map_iframe
        else:
            section_md += "\n\n_No location data linked to this submission._"
        sections.append(section_md)
    return "\n\n".join(sections)


def build_report_html(markdown_sections: Iterable[str]) -> str:
    md_text = "\n\n".join(section.strip() for section in markdown_sections if section)
    body = markdown(md_text, extensions=["extra", "sane_lists"])
    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\" />
<title>Participant report</title>
<link rel=\"preconnect\" href=\"https://unpkg.com\" />
<link rel=\"preconnect\" href=\"https://cdnjs.cloudflare.com\" />
<style>
body {{ font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2.5rem auto; max-width: 900px; padding: 0 1.5rem; line-height: 1.6; color: #1f2933; }}
h1, h2, h3 {{ color: #111827; margin-top: 2rem; }}
h1 {{ font-size: 2.1rem; }}
h2 {{ font-size: 1.5rem; }}
h3 {{ font-size: 1.25rem; }}
strong {{ color: #0f172a; }}
ul {{ padding-left: 1.2rem; }}
iframe {{ background: #f8fafc; }}
section {{ margin-bottom: 2rem; }}
</style>
</head>
<body>
{body}
</body>
</html>
"""


def generate_reports(args: argparse.Namespace) -> List[Path]:
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    maps_dir = output_dir / args.maps_subdir

    required_files = {
        "consent": input_dir / "consent.csv",
        "initial": input_dir / "initial_survey.csv",
        "biweekly": input_dir / "biweekly_survey.csv",
    }
    missing = [name for name, path in required_files.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing structured files: {', '.join(missing)}")

    consent_df = pd.read_csv(required_files["consent"])
    if consent_df.empty:
        print("⚠️ No consent records found; skipping report generation.")
        return []

    initial_df = (
        pd.read_csv(required_files["initial"]).fillna("")
        if (input_dir / "initial_survey.csv").exists()
        else pd.DataFrame()
    )
    biweekly_df = (
        pd.read_csv(required_files["biweekly"]).fillna("")
        if (input_dir / "biweekly_survey.csv").exists()
        else pd.DataFrame()
    )
    location_path = input_dir / "location_data.csv"
    location_df = pd.read_csv(location_path).fillna("") if location_path.exists() else pd.DataFrame()

    overrides = load_question_overrides(args.question_config)

    participants = build_participant_data(consent_df, initial_df, biweekly_df)
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_files: List[Path] = []
    tester_entries: List[Tuple[pd.Timestamp, str]] = []
    participant_entries: List[Tuple[pd.Timestamp, str]] = []

    for participant in participants:
        sections: List[str] = [f"# Participant report – {participant.participant_uuid}"]
        sections.append(render_consent_section(participant.consent, participant.signature))
        sections.append(render_initial_section(participant.initial, overrides.get("initial", {})))
        sections.append(
            render_biweekly_section(
                participant.biweekly,
                overrides.get("biweekly", {}),
                location_df,
                maps_dir,
                args.maps_subdir,
            )
        )
        report_html = build_report_html(sections)
        filename = safe_filename(participant.participant_uuid or "participant") + ".html"
        report_path = output_dir / filename
        report_path.write_text(report_html, encoding="utf-8")
        generated_files.append(report_path)
        signature = (participant.signature or "").strip()
        uuid_display = html.escape(participant.participant_uuid)
        signature_display = html.escape(signature) if signature else ""
        label = uuid_display
        if signature_display:
            label = f"{uuid_display} <span style=\"color:#6b7280;\">({signature_display})</span>"
        consent_display = format_timestamp(participant.consent_submitted_at)
        app_version = participant.consent.get("app_version") or "n/a"
        initial_status = "Yes" if participant.initial is not None else "No"
        biweekly_count = len(participant.biweekly)
        latest_biweekly_display = format_timestamp(participant.latest_biweekly_at)
        entry = (
            f"<li><a href=\"{filename}\">{label}</a>"
            f"<div class=\"meta\">Consent: {html.escape(consent_display)} &middot; "
            f"App version: {html.escape(str(app_version))} &middot; "
            f"Initial submitted: {initial_status} &middot; "
            f"Biweekly submissions: {biweekly_count} &middot; "
            f"Latest biweekly: {html.escape(latest_biweekly_display)}</div></li>"
        )
        sort_key = participant.consent_submitted_at
        if sort_key is None or pd.isna(sort_key):
            sort_key = pd.Timestamp.max
        signature_upper = signature.upper()
        if signature_upper == "TESTER":
            tester_entries.append((sort_key, entry))
        elif signature_upper.startswith("P4H"):
            participant_entries.append((sort_key, entry))
        else:
            participant_entries.append((sort_key, entry))
        print(f"📝 Generated report for participant {participant.participant_uuid}")

    if tester_entries or participant_entries:
        sections_html: List[str] = [
            "<h1>Participant reports</h1>",
            "<p>The following participants have consented and have reports available:</p>",
        ]
        if tester_entries:
            tester_entries.sort(key=lambda item: item[0])
            sections_html.append("<h2>Testers</h2>")
            sections_html.append("<ul>\n" + "\n".join(entry for _, entry in tester_entries) + "\n</ul>")
        if participant_entries:
            participant_entries.sort(key=lambda item: item[0])
            sections_html.append("<h2>Participants</h2>")
            sections_html.append("<ul>\n" + "\n".join(entry for _, entry in participant_entries) + "\n</ul>")

        index_html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>Participant reports</title>
<style>
body {{ font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2.5rem auto; max-width: 760px; padding: 0 1.5rem; color: #1f2933; }}
ul {{ line-height: 1.8; }}
li {{ margin-bottom: 1rem; }}
.meta {{ color: #6b7280; font-size: 0.875rem; margin-top: 0.35rem; }}
a {{ color: #1d4ed8; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
{content}
</body>
</html>
""".format(content="\n".join(sections_html))
        (output_dir / "index.html").write_text(index_html, encoding="utf-8")

    return generated_files


def main() -> None:
    args = parse_args()
    generated = generate_reports(args)
    if generated:
        print(f"✅ Generated {len(generated)} participant report(s) in {args.output}")
    else:
        print("⚠️ No reports generated (no consent records found).")


if __name__ == "__main__":
    main()
