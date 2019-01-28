package com.yutuer.http;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;
import com.yutuer.database.WikiDatabaseService;

public class HttpServerVerticle extends AbstractVerticle
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

	public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
	public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

	private String wikiDbQueue = "wikidb.queue";

	private WikiDatabaseService dbService;

	private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

	private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n" + "\n" + "Feel-free to write in Markdown!\n";

	@Override
	public void start(Future<Void> future) throws Exception
	{
		// 事件总线
		String wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");
		// 获取代理
		dbService = WikiDatabaseService.createProxy(vertx, wikiDbQueue);
		// http服务器
		HttpServer httpServer = vertx.createHttpServer();

		Router router = Router.router(vertx);
		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler);
		router.post("/delete").handler(this::pageDeletionHandler);

		int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8081);

		httpServer.requestHandler(router::accept).listen(portNumber, ar ->
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

	}

	private void indexHandler(RoutingContext routingContext)
	{
		dbService.fetchAllPages(reply ->
		{
			if (reply.succeeded())
			{
				routingContext.put("title", "Wiki home");
				routingContext.put("pages", body.getJsonArray("pages").getList());

				JsonArray result = reply.result();
			}
		});

		DeliveryOptions options = new DeliveryOptions().addHeader("action", "all-pages");
		vertx.eventBus().send(wikiDbQueue, new JsonObject(), options, reply ->
		{
			if (reply.succeeded())
			{
				JsonObject body = (JsonObject) reply.result().body();


				templateEngine.render(routingContext, "templates", "/index.ftl", ar ->
				{
					if (ar.succeeded())
					{
						routingContext.response().putHeader("Content-Type", "text/html");
						routingContext.response().end(ar.result());
					}
					else
					{
						routingContext.fail(ar.cause());
					}
				});
			}
			else
			{
				routingContext.fail(reply.cause());
			}
		});
	}

	private void pageRenderingHandler(RoutingContext routingContext)
	{
		String requestedPage = routingContext.request().getParam("page");
		JsonObject request = new JsonObject().put("page", requestedPage);
		DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-page");
		vertx.eventBus().send(wikiDbQueue, request, options, reply ->
		{
			if (reply.succeeded())
			{
				JsonObject body = (JsonObject) reply.result().body();
				boolean found = body.getBoolean("found");
				String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);

				routingContext.put("title", requestedPage);
				routingContext.put("id", body.getInteger("id", -1));
				routingContext.put("newPage", found ? "no" : "yes");
				routingContext.put("rawContent", rawContent);
				routingContext.put("content", Processor.process(rawContent));
				routingContext.put("timestamp", new Date().toString());

				templateEngine.render(routingContext, "templates", "/page.ftl", ar ->
				{
					if (ar.succeeded())
					{
						routingContext.response().putHeader("Content-Type", "text/html");
						routingContext.response().end(ar.result());
					}
					else
					{
						routingContext.fail(ar.cause());
					}
				});
			}
			else
			{
				routingContext.fail(reply.cause());
			}
		});
	}

	private void pageUpdateHandler(RoutingContext routingContext)
	{
		String title = routingContext.request().getParam("title");
		JsonObject request = new JsonObject().put("id", routingContext.request().getParam("id")).put("title", title)
				.put("markdown", routingContext.request().getParam("markdown"));
		DeliveryOptions options = new DeliveryOptions();
		if ("yes".equals(routingContext.request().getParam("newPage")))
		{
			options.addHeader("action", "create-page");
		}
		else
		{
			options.addHeader("action", "save-page");
		}

		vertx.eventBus().send(wikiDbQueue, request, options, reply ->
		{
			if (reply.succeeded())
			{
				routingContext.response().setStatusCode(303);
				routingContext.response().putHeader("Location", "/wiki/" + title);
				routingContext.response().end();
			}
			else
			{
				routingContext.fail(reply.cause());
			}
		});
	}

	private void pageCreateHandler(RoutingContext routingContext)
	{
		String pageName = routingContext.request().getParam("name");
		String location = "/wiki/" + pageName;
		if (pageName == null || pageName.isEmpty())
		{
			location = "/";
		}
		routingContext.response().setStatusCode(303);
		routingContext.response().putHeader("Location", location);
		routingContext.response().end();
	}

	private void pageDeletionHandler(RoutingContext routingContext)
	{
		String id = routingContext.request().getParam("id");
		JsonObject request = new JsonObject().put("id", id);
		DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-page");
		vertx.eventBus().send(wikiDbQueue, request, options, reply ->
		{
			if (reply.succeeded())
			{
				routingContext.response().setStatusCode(303);
				routingContext.response().putHeader("Location", "/");
				routingContext.response().end();
			}
			else
			{
				routingContext.fail(reply.cause());
			}
		});
	}
}
