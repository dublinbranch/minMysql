LIBS += -lmariadb

HEADERS += \
	$$PWD/MITLS.h \
    $$PWD/min_mysql.h  \
	$$PWD/utilityfunctions.h
    
SOURCES += \
    $$PWD/min_mysql.cpp \
     \
    $$PWD/utilityfunctions.cpp
DISTFILES += /
	$$PWD/README.md 
