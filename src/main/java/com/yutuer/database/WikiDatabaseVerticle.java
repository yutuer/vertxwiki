package com.yutuer.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.serviceproxy.ProxyHelper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiDatabaseVerticle extends AbstractVerticle
{
	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

	public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
	public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
	public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
	public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";
	public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

	private JDBCClient dbClient;

	enum SqlQuery
	{
		CREATE_PAGES_TABLE,
		ALL_PAGES,
		GET_PAGE,
		CREATE_PAGE,
		SAVE_PAGE,
		DELETE_PAGE
	}

	private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();

	private void loadSqlQueries() throws IOException
	{
		String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
		InputStream queriesInputStream;
		if (queriesFile != null)
		{
			queriesInputStream = new FileInputStream(queriesFile);
		}
		else
		{
			queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
		}

		Properties queriesProps = new Properties();
		queriesProps.load(queriesInputStream);
		queriesInputStream.close();
		sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
		sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
		sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"));
		sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
		sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
		sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));
	}

	@Override
	public void start(Future<Void> startFuture) throws Exception
	{
		/*
		 * Note: this uses blocking APIs, but data is small...
		 */
		loadSqlQueries();
		dbClient = JDBCClient.createShared(
				vertx,
				new JsonObject().put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
						.put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
						.put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

		WikiDatabaseService.create(dbClient, sqlQueries, ready ->
		{
			if (ready.succeeded())
			{
				ProxyHelper.registerService(WikiDatabaseService.class, vertx, ready.result(), CONFIG_WIKIDB_QUEUE);
				startFuture.complete();
			}
			else
			{
				startFuture.fail(ready.cause());
			}
		});
	}

	public enum ErrorCodes
	{
		NO_ACTION_SPECIFIED,
		BAD_ACTION,
		DB_ERROR
	}

}
