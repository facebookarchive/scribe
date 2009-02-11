@GLOBAL_HEADER_MK@

@PRODUCT_MK@

all:

# IMPORTANT THINGS TO FOLLOW
# Do not use relative paths to refer to sources, objects, libs etc not located in the current component dir or its subdirectories.
# Use $(top_srcdir), $(top_buildir), $(srcdir) which are provided by automake.
# Example:
#  Use /xxx/yyy OR $(xxx_home)/yyy/.. to refer to paths outside the root of this project.
# To refer to sources or other input files use $(top_srcdir)/... if outside of current dir but in this project.
# To refer to all *generated* files, objects, libs etc [ if not in current dir or its subdir], use $(top_builddir)
# Follow naming conventions for global, common and specific flags.
# Make build configuratiion and rules readable.
# Break up into logical sections.
# Set up command line options, default paths in configure.ac.
# Generate gen-cpp in current dir not in other locations.
# Do no create new targets especially if build time is small
# Default setting is opt mode. Use --disable-opt to turn this off.
# Default setting is static library . Use --disable-debug to turn on shared.

# Section 1 #########################################################################
# Set up User defined flags, variables and switches here.
# It is preferable to define these variables in configure.ac.

# User specified path variables set in configure.ac.
# thrift_home
# jvm_lib
# smc_home
# fb_home
# fb303_home
#
THRIFT = $(thrift_home)/bin/thrift

# User defined conditionals and conditonal statements set up in configure.ac.
# FACEBOOK set in configure.ac

if FACEBOOK
  FB_SOURCES = gen-cpp/ServiceManager_types.cpp gen-cpp/ServiceManager.cpp
  FB_CPPFLAGS = -I$(fb_home)
endif
if DEBUG
   DEBUG_CPPFLAGS = -DDEBUG_TIMING
endif


# Set libraries external to this component.
EXTERNAL_LIBS = -L$(thrift_home)/lib -L$(fb303_home)/lib -lfb303  -lthrift -lthriftnb
EXTERNAL_LIBS += -levent -lpthread -lrt

# Section 2 ############################################################################
# Set common flags recognized by automake.
# DO NOT USE CPPFLAGS, CXXFLAGS, CFLAGS, LDFLAGS here! Set in configure.ac and|or override on command line.
# USE flags AM_CXXFLAGS, AM_CFLAGS, AM_CPPFLAGS, AM_LDFLAGS, LDADD in this section.

AM_CPPFLAGS = -I..
AM_CPPFLAGS += -I$(thrift_home)/include
AM_CPPFLAGS += -I$(thrift_home)/include/thrift
AM_CPPFLAGS += -I$(fb303_home)/include/thrift
AM_CPPFLAGS += -I$(fb303_home)/include/thrift/fb303
AM_CPPFLAGS += $(BOOST_CPPFLAGS)
AM_CPPFLAGS += $(FB_CPPFLAGS) $(DEBUG_CPPFLAGS)

AM_LDFLAGS = $(BOOST_LDFLAGS) $(BOOST_SYSTEM_LIB) $(BOOST_FILESYSTEM_LIB)

# Section 3 #############################################################################
# GENERATE BUILD RULES
# Set Program/library specific flags recognized by automake.
# Use <progname|libname>_<FLAG> to set prog / lib specific flag s
# foo_CXXFLAGS foo_CPPFLAGS foo_LDFLAGS foo_LDADD

# Static -- multiple libraries can be defined
if STATIC
lib_LIBRARIES = libscribe.a
libscribe_a_SOURCES = gen-cpp/scribe.cpp gen-cpp/scribe_types.cpp gen-cpp/scribe_constants.cpp
INTERNAL_LIBS = libscribe.a
endif

# Shared -- multiple libraries can be defined
if SHARED
shareddir = lib
shared_PROGRAMS = libscribe.so
libscribe_so_SOURCES = gen-cpp/scribe.cpp gen-cpp/scribe_types.cpp
libscribe_so_CXXFLAGS = $(SHARED_CXXFLAGS)
libscribe_so_LDFLAGS = $(SHARED_LDFLAGS)
INTERNAL_LIBS =  libscribe.so
endif

# Binaries -- multiple progs can be defined.
bin_PROGRAMS = scribed
scribed_SOURCES = store.cpp store_queue.cpp conf.cpp file.cpp conn_pool.cpp scribe_server.cpp $(FB_SOURCES)
scribed_LDADD = $(EXTERNAL_LIBS) $(INTERNAL_LIBS)

if SHARED
scribed_DEPENDENCIES = libscribe.so
endif

# Section 4 ##############################################################################
# Set up Thrift specific activity here.
# We assume that a <name>+types.cpp will always be built from <name>.thrift.

$(eval $(call thrift_template,.,$(srcdir)/../if/scribe.thrift, -I $(fb303_home)/share/ --cpp --py --php))

if FACEBOOK
  $(eval $(call thrift_template,.,$(smc_home)/if/ServiceManager.thrift,--cpp))
endif

BUILT_SOURCES = thriftstyle

# Section 5 [OPTIONAL] ##################################################################################
# Create user specific targets [ OPTIONAL]

# Overrides pre-existing target "all". Use only if required.
# all: <all-special>

# Add to pre-existing target clean

clean-local: clean-common

# Add to pre-existing target all:
#all-local: scribed
#	@mkdir -p $(top_srcdir)/bin
#	cp $(bin_PROGRAMS) $(top_srcdir)/bin

# Special targets.
#server-opt : $(BUILT_SOURCES) scribed

@GLOBAL_FOOTER_MK@
