# Sentinel — one-command reproducibility.
# Windows users without `make` can run the underlying commands directly (see README).

PY ?= python

.PHONY: install test eval plots demo seed lint

install:            ## editable install with plotting extra
	pip install -e ".[viz]"

test:               ## run the test suite
	pytest -q

eval:               ## regenerate every evaluation number + chart (no API key needed)
	$(PY) -m evaluation.run_experiments
	$(PY) -m evaluation.graduated
	$(PY) -m evaluation.plots

plots:              ## re-render docs/*.png from data/graduated_eval.json
	$(PY) -m evaluation.plots

seed:               ## populate the live dashboard DB (rules-only, no API key)
	$(PY) scripts/seed_demo.py --fresh

demo: seed          ## seed + launch the Streamlit dashboard
	streamlit run dashboard/app.py

lint:               ## ruff
	ruff check .
