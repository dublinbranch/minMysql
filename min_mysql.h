#ifndef MIN_MYSQL_H
#define MIN_MYSQL_H

#include <QStringList>

const QString    SQL_NULL  = "NULL";
const QByteArray BSQL_NULL = "NULL";
const QByteArray BZero     = "0";

QString base64this(const char* param);
QString base64this(const QByteArray& param);
QString base64this(const QString& param);
QString mayBeBase64(const QString& original);

struct st_mysql;
using sqlRow    = QMap<QByteArray, QByteArray>;
using sqlResult = QList<sqlRow>;
sqlResult MySQL_query(st_mysql* conn, const QByteArray& sql);
sqlResult MySQL_query(st_mysql* conn, const QString& sql);
sqlResult query(st_mysql* conn, const QString& sql);
sqlResult query(st_mysql* conn, const QByteArray& sql);

struct DB {
	QByteArray        host = "127.0.0.1";
	QByteArray        pass;
	QByteArray        user;
	QString           defaultDB;
	uint              port = 3306;
	mutable st_mysql* conn = nullptr;

	void      connect();
	sqlResult query(const QString& sql) const;
	sqlResult query(const QByteArray& sql) const;
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
	const DB*   conn = nullptr;
	QStringList buffer;
	int         bufferSize = 50;

      public:
	SQLBuffering(DB* _conn, int _bufferSize = 50);
	~SQLBuffering();
	void append(const QString& sql);
	void flush();
};
#endif // MIN_MYSQL_H
