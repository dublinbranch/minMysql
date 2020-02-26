#ifndef MIN_MYSQL_H
#define MIN_MYSQL_H

#include "MITLS.h"
#include <QStringList>

//Those variable are shared in many places, order of initialization is important!
//Inline will avoid to have multiple copy, and enforces having a single one
inline const QString    mysqlDateFormat     = "yyyy-MM-dd";
inline const QString    mysqlDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
inline const QString    SQL_NULL            = "NULL";
inline const QByteArray BSQL_NULL           = "NULL";
inline const QByteArray BZero               = "0";
inline const QString    Zero                = "0";

QString base64this(const char* param);
QString base64this(const QByteArray& param);
QString base64this(const QString& param);
QString mayBeBase64(const QString& original);
QString base64Nullable(const QString& param);

struct st_mysql;
struct st_mysql_res;
using sqlRow    = QMap<QByteArray, QByteArray>;
using sqlResult = QList<sqlRow>;

struct SQLLogger {
	SQLLogger(const QByteArray& _sql, bool _enabled)
		: sql(_sql), enabled(_enabled) {
	}
	void flush();
	~SQLLogger();
	bool              flushed = false;
	const QByteArray& sql;
	const sqlResult*  res = nullptr;
	QString           error;
	bool              enabled = false;
};

/**
 * @brief The DB struct
 * TODO:
 * detach CONST config from actual class (mutable and const are code smell -.-)
 * just disable copy operator to avoid improper usage, if you pass a DB obj to another thread
 * you are CRAZY! no need to overcomplicate life with mi_tls ?
 * This can also help avoid the insanity of SQLLogger
 * CONST conf -> dynamic connection -> NON Reusable class ?
 */
class FetchVisitor;
struct DB {
      public:
	QByteArray host = "127.0.0.1";
	QByteArray pass;
	QByteArray user;

	uint      port = 3306;
	st_mysql* connect() const;
	sqlResult query(const QString& sql) const;
	sqlResult query(const QByteArray& sql) const;
	sqlResult query(const char* sql) const;
	/**
	  Those 2 are used toghether for the ASYNC mode
	 * @brief startQuery
	 * @param sql
	 */
	void startQuery(const QByteArray& sql) const;
	void startQuery(const QString& sql) const;
	void startQuery(const char* sql) const;
	bool completedQuery() const;

	//Shared by both async and not
	sqlResult fetchResult(SQLLogger* sqlLogger = nullptr) const;
	int       fetchAdvanced(FetchVisitor* visitor) const;
	st_mysql* getConn() const;
	ulong     lastId() const;
	long      affectedRows() const;

	//Non copyable
	DB()        = default;
	DB& operator=(const DB&) = delete;
	DB(const DB&)            = delete;

	QString getDefaultDB() const;
	void    setDefaultDB(const QString& value);
	bool    saveQuery = false;

      private:
	//Each thread and each instance will need it's own copy
	mutable mi_tls<st_mysql*> connPool;
	QString                   defaultDB;
	//user for asyncs
	mutable int        signalMask;
	mutable QByteArray lastSQL;
};

typedef char** MYSQL_ROW;
class FetchVisitor {
	  public:
	virtual void processLine(MYSQL_ROW row) = 0;
	virtual bool preCheck(st_mysql_res* result) = 0;
};

QString    QV(const sqlRow& line, const QByteArray& b);
QByteArray Q8(const sqlRow& line, const QByteArray& b);
QString    Q64(const sqlRow& line, const QByteArray& b);
quint64    getId(const sqlResult& res);

/**
 * @brief The SQLBuffering class it is a set of SQL queries with a flashing system which allows to execute
 * the queries (manually or automatically)
 */
class SQLBuffering {
	DB*         conn = nullptr;
	QStringList buffer;
	int         bufferSize = 1000;

      public:
	/**
	 * @brief SQLBuffering
	 * @param _conn
	 * @param _bufferSize 0 disable flushing, 1 disable buffering
	 */
	SQLBuffering(DB* _conn, int _bufferSize = 1000);
	SQLBuffering() = default;
	~SQLBuffering();
	void append(const QString& sql);
	void flush();
};

#endif // MIN_MYSQL_H
