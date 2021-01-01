LIBS += -lmariadb

HEADERS += \
	$$PWD/MITLS.h \
    $$PWD/min_mysql.h  \
    $$PWD/ttlcache.h \
	$$PWD/utilityfunctions.h
    
SOURCES += \
    $$PWD/min_mysql.cpp \
    $$PWD/ttlcache.cpp \
     \
    $$PWD/utilityfunctions.cpp
DISTFILES += /
	$$PWD/README.md 
