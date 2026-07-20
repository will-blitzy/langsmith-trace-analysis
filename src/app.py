#!/usr/bin/env python3
"""Streamlit UI for LangSmith trace export."""
import os
import sys
import subprocess

import streamlit as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, ".env"))
except ImportError:
    pass

st.set_page_config(page_title="LangSmith Trace Export", layout="centered")
st.title("LangSmith Trace Export")

if not os.environ.get("LANGSMITH_API_KEY"):
    st.warning("LANGSMITH_API_KEY not set — add it to .env before exporting.")

mode = st.radio("Export mode", ["Trace tree", "Run IDs", "Project"], horizontal=True)

col1, col2 = st.columns(2)
with col1:
    name = st.text_input("Export name", placeholder="my-export")
with col2:
    max_mb = st.number_input("Max MB per file", min_value=1, value=30)

if mode == "Trace tree":
    trace_id = st.text_input("Root run ID")
elif mode == "Run IDs":
    run_ids_raw = st.text_area("Run IDs (one per line)")
else:
    project = st.text_input("Project name")
    col3, col4 = st.columns(2)
    with col3:
        limit = st.number_input("Limit", min_value=1, value=25)
    with col4:
        root_only = st.checkbox("Root runs only")

no_resolve_blobs = st.checkbox("Skip blob resolution (faster, leaves inputs/outputs empty)")

if st.button("Run Export", type="primary", disabled=not name):
    cmd = [sys.executable, os.path.join(ROOT, "src", "export.py"),
           "--name", name, "--max-mb", str(max_mb)]

    if mode == "Trace tree":
        if not trace_id.strip():
            st.error("Root run ID is required.")
            st.stop()
        cmd += ["--trace", trace_id.strip()]
    elif mode == "Run IDs":
        ids = [x.strip() for x in run_ids_raw.splitlines() if x.strip()]
        if not ids:
            st.error("Enter at least one run ID.")
            st.stop()
        cmd += ["--run-ids"] + ids
    else:
        if not project.strip():
            st.error("Project name is required.")
            st.stop()
        cmd += ["--project", project.strip(), "--limit", str(limit)]
        if root_only:
            cmd += ["--root-only"]

    if no_resolve_blobs:
        cmd += ["--no-resolve-blobs"]

    log_area = st.empty()
    log_lines = []

    with st.spinner("Running export…"):
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=ROOT,
        )
        for line in proc.stdout:
            log_lines.append(line.rstrip())
            log_area.code("\n".join(log_lines))
        proc.wait()

    if proc.returncode != 0:
        st.error("Export failed.")
    else:
        st.success("Export complete!")
        export_dir = os.path.join(ROOT, "exports", name)
        if os.path.isdir(export_dir):
            files = sorted(f for f in os.listdir(export_dir) if f.endswith(".json"))
            for fname in files:
                with open(os.path.join(export_dir, fname), "rb") as f:
                    st.download_button(
                        label=f"Download {fname}",
                        data=f.read(),
                        file_name=fname,
                        mime="application/json",
                    )
