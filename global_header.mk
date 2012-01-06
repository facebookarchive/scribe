#define thrift_template
#  $(1) : $(2)
#	$$(THRIFT) $(3) $(4) $(5) $(6) $(7) $(8) $$<
#endef

define thrift_template
XTARGET := $(shell perl -e '$$_ = $$ARGV[1]; s{^.*/}{}; s{\..*}{}; print "$$ARGV[0]/gen-cpp/$${_}_types.cpp\n"' '$(1)' '$(2)')

ifneq ($$(XBUILT_SOURCES),) 
    XBUILT_SOURCES := $$(XBUILT_SOURCES) $$(XTARGET)
else
    XBUILT_SOURCES := $$(XTARGET)
endif
$$(XTARGET) : $(2)
	$$(THRIFT) -o $1 $3 $$<
endef

clean-common:
	rm -rf gen-*
