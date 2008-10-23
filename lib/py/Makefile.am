DESTDIR ?= /
EXTRA_DIST = setup.py scribe

all:

all-local:
	$(PYTHON) setup.py build

# We're ignoring prefix here because site-packages seems to be
# the equivalent of /usr/local/lib in Python land.
# Old version (can't put inline because it's not portable).
#$(PYTHON) setup.py install --prefix=$(prefix) --root=$(DESTDIR) $(PYTHON_SETUPUTIL_ARGS)
install-exec-hook:
	$(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PY_PREFIX) $(PYTHON_SETUPUTIL_ARGS)



clean:	clean-local

clean-local:

check-local: all
