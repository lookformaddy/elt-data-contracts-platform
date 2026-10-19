.PHONY: test demo serve contracts clean

test:
	PYTHONPATH=src python -m pytest -q

demo:
	python examples/run_demo.py

serve:
	elt-demo serve --port 8000

contracts:
	elt-demo contracts

clean:
	rm -rf elt-demo-data .pytest_cache
