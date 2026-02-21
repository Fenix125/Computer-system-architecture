PYTHON ?= .venv/bin/python

.PHONY: run-facade run-logging run-counter run-all

run-facade:
	$(PYTHON) -m services.facade_service.main

run-logging:
	$(PYTHON) -m services.logging_service.main

run-counter:
	$(PYTHON) -m services.counter_service.main

run-all:
	@set -e; \
	$(PYTHON) -m services.logging_service.main & LOG_PID=$$!; \
	$(PYTHON) -m services.counter_service.main & CNT_PID=$$!; \
	$(PYTHON) -m services.facade_service.main & FAC_PID=$$!; \
	trap 'kill $$LOG_PID $$CNT_PID $$FAC_PID' INT TERM EXIT; \
	wait
