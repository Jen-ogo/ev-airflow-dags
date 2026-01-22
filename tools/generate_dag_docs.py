from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

from airflow.models import DagBag

def md_escape(s: str) -> str:
    return (s or "").replace("|", "\\|")

def main():
    dags_dir = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))
    docs_dir = Path(os.environ.get("DAG_DOCS_DIR", "/opt/airflow/docs/dags"))
    docs_dir.mkdir(parents=True, exist_ok=True)

    bag = DagBag(dag_folder=str(dags_dir), include_examples=False)

    index_lines = [
        "# DAGs",
        "",
        f"_Generated: {datetime.utcnow().isoformat()}Z_",
        "",
        "| dag_id | schedule | tags | file |",
        "|---|---|---|---|",
    ]

    for dag_id, dag in sorted(bag.dags.items()):
        schedule = md_escape(str(getattr(dag, "schedule_interval", None)))
        tags = md_escape(", ".join(getattr(dag, "tags", []) or []))
        fileloc = str(getattr(dag, "fileloc", ""))
        rel_file = md_escape(fileloc.replace(str(dags_dir) + "/", ""))

        index_lines.append(f"| `{dag_id}` | `{schedule}` | {tags} | `{rel_file}` |")

        out = docs_dir / f"{dag_id}.md"
        lines = [
            f"# {dag_id}",
            "",
            f"**Schedule:** `{schedule}`",
            f"**Tags:** {tags}",
            f"**File:** `{rel_file}`",
            "",
            "## Tasks",
            "",
            "| task_id | operator | trigger_rule |",
            "|---|---|---|",
        ]

        for t in dag.tasks:
            op = md_escape(t.__class__.__name__)
            tr = md_escape(getattr(t, "trigger_rule", ""))
            lines.append(f"| `{t.task_id}` | `{op}` | `{tr}` |")

        lines += [
            "",
            "## Notes",
            "",
            "- Inputs / outputs / ADLS paths: fill manually here (project-specific).",
        ]

        out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    (docs_dir / "README.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    print(f"Generated docs into: {docs_dir}")

if __name__ == "__main__":
    main()