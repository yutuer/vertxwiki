package com.yutuer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle
{
	private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
	private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
	private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
	private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
	private static final String SQL_ALL_PAGES = "select Name from Pages";
	private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

	private JDBCClient dbClient;

	private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

	@Override
	public void start(Future<Void> startFuture) throws Exception
	{
		Future<Void> steps = prepareDB().compose(v -> prepareHttpServer());
		steps.setHandler(startFuture.completer());
	}

	private Future<Void> prepareDB()
	{
		Future<Void> future = Future.future();

		JsonObject config = new JsonObject().put("url", "jdbc:hsqldb:file:db/wiki").put("driver_class", "org.hsqldb.jdbcDriver")
				.put("max_pool_size", 30);
		dbClient = JDBCClient.createShared(vertx, config);

		dbClient.getConnection(ar ->
		{
			if (ar.succeeded())
			{
				SQLConnection connection = ar.result();
				connection.execute(SQL_CREATE_PAGES_TABLE, create ->
				{
					connection.close();
					if (create.succeeded())
					{
						LOGGER.info("Database preparation success!!!!");
						future.complete();
					}
					else
					{
						LOGGER.error("Database preparation error", create.cause());
						future.fail(ar.cause());
					}
				});
			}
			else
			{
				LOGGER.error("Could not open a database connection", ar.cause());
				future.fail(ar.cause());
			}
		});

		return future;
	}

	private Future<Void> prepareHttpServer()
	{
		Future<Void> future = Future.future();

		HttpServer httpServer = vertx.createHttpServer();

		Router router = Router.router(vertx);
		router.get("/").handler(this::indexHandler);

		httpServer.requestHandler(router::accept).listen(8081, ar ->
		{
			if (ar.succeeded())
			{
				LOGGER.info("HttpServer listen preparation success!!!!");
				future.complete();
			}
			else
			{
				LOGGER.error("HttpServer preparation error", ar.cause());
				future.fail(ar.cause());
			}
		});

		return future;
	}

	private void indexHandler(RoutingContext rc)
	{

	}
}
